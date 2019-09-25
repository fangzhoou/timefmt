package core

import (
    "context"
    "errors"
    "fmt"
    "os"
    "runtime/debug"
    "sync"
    "time"

    "github.com/coreos/etcd/mvcc/mvccpb"
    "github.com/fangzhoou/dcron/utils"
    "go.etcd.io/etcd/clientv3"
)

// 服务注册 etcd 的租约时间，3 秒
const LeaseTime = 3

// etcd client
var etcdClient *clientv3.Client

// 初始化 etcd
// 利用 etcd 实现服务发现和
func InitEtcd(sc StopChan) {
    if len(Conf.EtcdEndpoints) == 0 {
        // 没有配 etcd 则返回空，服务为单机版
        return
    }

    defer func() {
        if r := recover(); r != nil {
            fmt.Printf("Etcd fatal: %v\n", r)
            debug.PrintStack()
            os.Exit(-2)
        }
    }()

    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   Conf.EtcdEndpoints,
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        panic(err)
    }
    etcdClient = cli

    // 服务注册与监控
    err = registerAndWatch(sc)
    if err != nil {
        panic(err)
    }
}

// 注册服务及心跳监控
func registerAndWatch(sc StopChan) error {
    // 注册服务
    ip, err := utils.GetLocalIP()
    if err != nil {
        return err
    }
    s := NewServer(ip)
    err = s.register()
    if err != nil {
        return err
    }
    s.keepAlive(sc)

    // 获取所有服务结点
    m := &master{NodeList: map[string]string{}}
    m.getNodeList()

    // 启动服务监控
    go m.watchServers()
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
func (s *server) register() error {
    // 查看服务是否存在
    key := Conf.Name + "/node/" + s.Name
    resp, err := etcdClient.Get(context.TODO(), key)
    if err != nil {
        return err
    }
    if resp.Count > 0 {
        return errors.New(`server "` + key + `"  already registered in etcd`)
    }

    // 建立 etcd 租约关系
    gResp, err := etcdClient.Grant(context.TODO(), LeaseTime)
    if err != nil {
        return err
    }
    s.LeaseId = gResp.ID
    _, err = etcdClient.Put(context.TODO(), getNodePrefix()+s.Name, s.IP, clientv3.WithLease(s.LeaseId))
    if err != nil {
        return err
    }
    return nil
}

// 服务心跳
func (s *server) keepAlive(sc StopChan) {
    ka, err := etcdClient.KeepAlive(context.TODO(), s.LeaseId)
    if err != nil {
        fmt.Errorf(err.Error())
        sc.Done()
        return
    }
    go func() {
        for {
            select {
            case re := <-ka:
                if re == nil {
                    _, err := etcdClient.Revoke(context.TODO(), s.LeaseId)
                    if err != nil {
                        fmt.Errorf(err.Error())
                        sc.Done()
                        return
                    }
                }
            }
        }
    }()
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
