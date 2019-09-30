package core

import (
    "context"
    "time"

    "go.etcd.io/etcd/clientv3"
)

const (
    // etcd 链接超时时间，默认 5 秒
    DialTimeOut = 5 * time.Second

    // 锁的租约时长
    MutexLeaseTime int64 = 3
)

// etcd 服务
type etcd struct {
    Cli *clientv3.Client
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
        DialTimeout: DialTimeOut,
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

// etcd 互斥锁
// 利用 etcd 的租约特性实现锁机制，
// 上锁时创建一个固定时长的租约，解锁时释放这个租约
type etcdMutex struct {
    // etcd 锁的 key
    Key string

    // 租约时长
    Ttl int64

    // etcd 租约对象
    Lease clientv3.Lease

    // 租约 id
    LeaseId clientv3.LeaseID

    // etcd txn
    Txn clientv3.Txn

    // 关闭租约方法
    Cancel context.CancelFunc
}

// 创建 etcd 锁
func NewEtcdMutex(key string) (*etcdMutex, error) {
    em := &etcdMutex{
        Key: key,
        Ttl: MutexLeaseTime,
    }
    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   Conf.EtcdEndpoints,
        DialTimeout: DialTimeOut,
    })
    if err != nil {
        return em, err
    }

    em.Lease = clientv3.NewLease(cli)
    resp, err := em.Lease.Grant(context.TODO(), em.Ttl)
    if err != nil {
        return em, err
    }
    em.LeaseId = resp.ID
    em.Txn = clientv3.NewKV(cli).Txn(context.TODO())
    var ctx context.Context
    ctx, em.Cancel = context.WithCancel(context.TODO())
    _, err = em.Lease.KeepAlive(ctx, em.LeaseId)
    return em, nil
}

// etcd 上锁
func (em *etcdMutex) Lock() {
    em.Txn.If(clientv3.Compare(clientv3.CreateRevision(em.Key), "=", 0)).
        Then(clientv3.OpPut(em.Key, "", clientv3.WithLease(em.LeaseId))).
        Else()

    for {
        txnResp, err := em.Txn.Commit()
        if err != nil {
            log.Error("get etcd lock failed：", err.Error())
            panic(err)
        }
        if txnResp.Succeeded {
            return
        }
    }
}

// etcd 解锁
func (em *etcdMutex) Unlock() {
    em.Cancel()
    em.Lease.Revoke(context.TODO(), em.LeaseId)
}
