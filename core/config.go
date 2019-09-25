package core

import (
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
    "runtime/debug"

    "github.com/BurntSushi/toml"
)

// 配置文件
type config struct {
    // 应用名称
    Name string

    // http 服务端口
    Port int

    // 数据存储路径
    Storage string

    // etcd 结点配置地址
    EtcdEndpoints []string `toml:"etcd_endpoints"`
}

var Conf config

// 加载配置文件
func LoadConfig() {
    defer func() {
        if r := recover(); r != nil {
            fmt.Printf("Config fatal: %v\n", r)
            debug.PrintStack()
            os.Exit(-2)
        }
    }()

    rootPath, err := os.Getwd()
    if err != nil {
        panic(err)
    }
    confFile, err := filepath.Abs(rootPath + "/conf/config.toml")
    if err != nil {
        panic(err)
    }
    tomlData, err := ioutil.ReadFile(confFile)
    if err != nil {
        panic(err)
    }
    if _, err := toml.Decode(string(tomlData), &Conf); err != nil {
        panic("toml decode config data failed: " + err.Error())
    }
}
