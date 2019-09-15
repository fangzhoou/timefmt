package core

import (
    "time"

    "go.etcd.io/etcd/clientv3"
)

type etcd struct {
    Client *clientv3.Client
}

var Etcd etcd

func init() {
    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   Conf.EtcdEndpoints,
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        panic(err)
    }
    Etcd.Client = cli
}
