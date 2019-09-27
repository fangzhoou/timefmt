package core

import (
    "context"
    "errors"
    "sync"
    "time"

    "github.com/coreos/etcd/mvcc/mvccpb"
    "github.com/fangzhoou/dcron/utils"
    "go.etcd.io/etcd/clientv3"
)

// 服务注册 etcd 的租约时间，默认 3 秒
const LeaseTime = 3

// etcd 服务
type etcd struct {
    Status int
    Cli    *clientv3.Client
}

var Etcd = &etcd{}

// 初始化 etcd，没有配置 etcd 则服务为单机版
// 利用 etcd 实现服务发现和服务治理
func InitEtcd(ctx context.Context) error {
    // 没有配置 etcd 返回空
    if len(Conf.EtcdEndpoints) == 0 {
        return nil
    }

    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   Conf.EtcdEndpoints,
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        return err
    }
    Etcd.Cli = cli

    // 服务注册与监控
    err = registerAndWatch(ctx)
    if err != nil {
        return err
    }
    return nil
}

// 注册服务及心跳监控
func registerAndWatch(ctx context.Context) error {
    // 注册服务
    ip, err := utils.GetLocalIP()
    if err != nil {
        return err
    }
    s := NewServer(ip)
    err = s.register(ctx)
    if err != nil {
        return err
    }
    go s.keepAlive(ctx)

    // 获取所有服务结点
    m := &master{NodeList: map[string]string{}}
    err = m.getNodeList(ctx)
    if err != nil {
        return err
    }

    // 启动服务监控
    go m.watchServers(ctx)
    return nil
}

// 获取服务节点前缀
func getNodePrefix() string {
    return Conf.Name + "/node/"
}

// 服务对象
type server struct {
    Name    string
    IP      string
    LeaseId clientv3.LeaseID
}

func NewServer(ip string) *server {
    return &server{Name: utils.Md5(ip), IP: ip}
}

// 注册服务
func (s *server) register(ctx context.Context) error {
    // 查看服务是否存在
    key := Conf.Name + "/node/" + s.Name
    resp, err := Etcd.Cli.Get(ctx, key)
    if err != nil {
        return err
    }
    if resp.Count > 0 {
        return errors.New(`server "` + key + `"  already registered in etcd`)
    }

    // 建立 etcd 租约关系
    gResp, err := Etcd.Cli.Grant(ctx, LeaseTime)
    if err != nil {
        return err
    }
    s.LeaseId = gResp.ID
    _, err = Etcd.Cli.Put(ctx, getNodePrefix()+s.Name, s.IP, clientv3.WithLease(s.LeaseId))
    if err != nil {
        return err
    }
    return nil
}

// 服务心跳
func (s *server) keepAlive(ctx context.Context) {
    ka, err := Etcd.Cli.KeepAlive(ctx, s.LeaseId)
    if err != nil {
        log.Error(err)
        CronCancel()
        return
    }
    for {
        select {
        case re := <-ka:
            if re == nil {
                _, err := Etcd.Cli.Revoke(ctx, s.LeaseId)
                if err != nil {
                    log.Error(err)
                    CronCancel()
                    return
                }
            }
        }
    }
}

// 服务监控
type master struct {
    NodeList map[string]string

    mu sync.Mutex
}

// 获取服务列表
func (m *master) getNodeList(ctx context.Context) error {
    resp, err := Etcd.Cli.Get(ctx, getNodePrefix(), clientv3.WithPrefix())
    if err != nil {
        return err
    }
    for _, kv := range resp.Kvs {
        m.NodeList[string(kv.Key)] = string(kv.Value)
    }
    return nil
}

// 监控服务列表，服务节点有变动时更新
func (m *master) watchServers(ctx context.Context) {
    wc := Etcd.Cli.Watch(ctx, getNodePrefix(), clientv3.WithPrefix())
    for {
        select {
        case resp := <-wc:
            if resp.Err() != nil {
                log.Error(resp.Err())
                CronCancel()
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
