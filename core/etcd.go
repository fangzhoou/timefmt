package core

import (
    "time"

    "go.etcd.io/etcd/clientv3"
)

var etcdClient *clientv3.Client

// 返回 etcd client
func GetEtcdClient() *clientv3.Client {
    return etcdClient
}

// 建立 etcd 链接
func NewEtcd() *clientv3.Client {
    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   Conf.EtcdEndpoints,
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        panic(err)
    }
    etcdClient = cli
    return cli
}
