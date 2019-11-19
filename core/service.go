package core

import (
    "context"
    "errors"
    "sync"

    "github.com/coreos/etcd/mvcc/mvccpb"
    "github.com/shirou/gopsutil/cpu"
    "github.com/shirou/gopsutil/mem"
    "go.etcd.io/etcd/clientv3"
)

const (
    // 服务注册 etcd 的租约时间，默认 3 秒
    LeaseTime = 3
)

// 服务对象
type server struct {
    Name    string
    IP      string
    LeaseId clientv3.LeaseID
}

// 注册服务
func (s *server) register(ctx context.Context) error {
    // 查看服务是否存在
    key := getNodePrefix() + s.Name
    resp, err := Etcd().Cli.Get(ctx, key)
    if err != nil {
        return err
    }
    if resp.Count > 0 {
        kv := resp.Kvs[0]
        return errors.New(`server "` + string(kv.Value) + `" already registered in etcd. key: ` + key)
    }

    // 建立 etcd 租约关系
    gResp, err := Etcd().Cli.Grant(ctx, LeaseTime)
    if err != nil {
        return err
    }
    s.LeaseId = gResp.ID
    _, err = Etcd().Cli.Put(ctx, getNodePrefix()+s.Name, s.IP, clientv3.WithLease(s.LeaseId))
    if err != nil {
        return err
    }

    return nil
}

// 服务心跳
func (s *server) keepAlive(ctx context.Context) {
    ka, err := Etcd().Cli.KeepAlive(ctx, s.LeaseId)
    if err != nil {
        log.Error(err)
        Cron.Cancel()
        return
    }
    for {
        select {
        case re := <-ka:
            if re == nil {
                _, err := Etcd().Cli.Revoke(ctx, s.LeaseId)
                if err != nil {
                    log.Error(err)
                    Cron.Cancel()
                    return
                }
            } else {
                // 同步服务器状态
                status, err := getMachineStatus(ctx)
                if err != nil {
                    log.Error(err)
                    Cron.Cancel()
                    return
                }
                _, err = Etcd().Cli.Put(ctx, getNodePrefix()+s.Name+"/status", status, clientv3.WithLease(s.LeaseId))
                if err != nil {
                    log.Error(err)
                    Cron.Cancel()
                    return
                }
            }
        }
    }
}

// 服务监控
type master struct {
    NodeList map[string]string
    NodeNum  int
    mu       sync.Mutex
}

// 获取服务列表
func (m *master) getNodeList(ctx context.Context) error {
    resp, err := Etcd().Cli.Get(ctx, getNodePrefix(), clientv3.WithPrefix())
    if err != nil {
        return err
    }
    for k, kv := range resp.Kvs {
        m.NodeList[string(kv.Key)] = string(kv.Value)
        m.NodeNum = k + 1
    }
    return nil
}

// 监控服务列表，服务节点有变动时更新
func (m *master) watchServers(ctx context.Context) {
    wc := Etcd().Cli.Watch(ctx, getNodePrefix(), clientv3.WithPrefix())
    for {
        select {
        case resp := <-wc:
            if resp.Err() != nil {
                log.Error(resp.Err())
                Cron.Cancel()
            }
            for _, ev := range resp.Events {
                switch ev.Type {
                case mvccpb.PUT:
                    m.mu.Lock()
                    m.NodeList[string(ev.Kv.Key)] = string(ev.Kv.Value)
                    m.mu.Unlock()
                case mvccpb.DELETE:
                    if _, ok := m.NodeList[string(ev.Kv.Key)]; ok {
                        m.mu.Lock()
                        delete(m.NodeList, string(ev.Kv.Key))
                        m.mu.Unlock()
                    }
                }
            }
        }
    }
}

var (
    serverInstance *server
    masterInstance *master

    onceS sync.Once
    onceM sync.Once
)

// 当前服务对象
func Server() *server {
    onceS.Do(func() {
        ip := GetLocalIP()
        serverInstance = &server{
            Name: Md5(ip),
            IP:   ip,
        }
    })
    return serverInstance
}

// 服务管理对象
func Master() *master {
    onceM.Do(func() {
        masterInstance = &master{NodeList: map[string]string{}}
    })
    return masterInstance
}

// 注册服务及心跳监控
func registerAndWatch(ctx context.Context) error {
    // 注册服务
    err := Server().register(ctx)
    if err != nil {
        return err
    }
    go Server().keepAlive(ctx)

    // 获取所有服务结点
    err = Master().getNodeList(ctx)
    if err != nil {
        return err
    }

    // 启动服务监控
    go Master().watchServers(ctx)
    return nil
}

// 获取服务节点前缀
func getNodePrefix() string {
    return Conf.Name + "/node/"
}

// 获取当前机器运行状态
// 状态值 = (cpu利用率 * 0.8 + 内存利用率 * 0.2) * 100
// 状态值越低证明机器越空闲，分配任务时优先选择
func getMachineStatus(ctx context.Context) (int, error) {
    var status int

    // 获取物理 cpu 使用率
    cp, err := cpu.PercentWithContext(ctx, 0, false)
    if err != nil {
        return status, err
    }

    // 内存利用率
    m, err := mem.VirtualMemoryWithContext(ctx)
    if err != nil {
        return status, err
    }

    status = int((cp[0]*0.8 + m.UsedPercent*0.2) * 100)
    return status, nil
}
