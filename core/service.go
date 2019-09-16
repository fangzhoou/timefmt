package core

import (
    "context"
    "errors"
    "sync"

    "github.com/coreos/etcd/mvcc/mvccpb"
    "github.com/fangzhoou/dcron/utils"
    "go.etcd.io/etcd/clientv3"
)

// 服务注册 etcd 的租约时间，3 秒
const LeaseTime = 3

// 服务对象
type server struct {
    Name    string
    IP      string
    LeaseId clientv3.LeaseID
}

// 注册服务
func (s *server) register() {
    // 查看服务是否存在
    key := Conf.Name + "/node/" + s.Name
    resp, err := etcdClient.Get(context.TODO(), key)
    if err != nil {
        panic(err)
    }
    if resp.Count > 0 {
        panic(errors.New("server " + key + " already exists"))
    }

    // 建立 etcd 租约关系
    gResp, err := etcdClient.Grant(context.TODO(), LeaseTime)
    if err != nil {
        panic(err)
    }
    s.LeaseId = gResp.ID
    _, err = etcdClient.Put(context.TODO(), getNodePrefix()+s.Name, s.IP, clientv3.WithLease(s.LeaseId))
    if err != nil {
        panic(err)
    }
}

// 服务心跳
func (s *server) keepAlive() {
    for {
        ka, err := etcdClient.KeepAlive(context.TODO(), s.LeaseId)
        if err != nil {
            panic(err)
        }
        select {
        case re := <-ka:
            if re == nil {
                _, err := etcdClient.Revoke(context.TODO(), s.LeaseId)
                if err != nil {
                    panic(err)
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
func (m *master) getNodeList() {
    resp, err := etcdClient.Get(context.TODO(), getNodePrefix(), clientv3.WithPrefix())
    if err != nil {
        panic(err)
    }
    for _, kv := range resp.Kvs {
        m.NodeList[string(kv.Key)] = string(kv.Value)
    }
}

// 监控服务列表，服务节点有变动时更新
func (m *master) watchServers() {
    for {
        wc := etcdClient.Watch(context.TODO(), getNodePrefix(), clientv3.WithPrefix())
        select {
        case resp := <-wc:
            if resp.Err() != nil {
                panic(resp.Err())
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

// 注册服务
func RegisterAndWatch() {
    ip, err := utils.GetLocalIP()
    if err != nil {
        panic(err)
    }
    s := &server{Name: utils.Md5(ip), IP: ip}
    s.register()

    // 保持服务心跳
    go s.keepAlive()

    // 获取所有服务结点
    m := &master{NodeList: map[string]string{}}
    m.getNodeList()

    // 注册成功启动服务发现
    go m.watchServers()
}

// 获取服务节点前缀
func getNodePrefix() string {
    return Conf.Name + "/node/"
}
