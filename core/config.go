package core

import (
    "io/ioutil"
    "os"
    "path/filepath"

    "github.com/BurntSushi/toml"
)

// 配置文件
type config struct {
    Name          string
    Port          string
    EtcdEndpoints string `toml:"etcd_endpoints"`
}

var Conf config

// 加载配置文件
func LoadConfig() {
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
        panic("load config failed")
    }
}
