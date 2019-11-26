package core

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// 默认 http 服务端口
const DefaultPort int = 7777

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

var Conf = config{
	Name:          "dcron",
	Port:          DefaultPort,
	Storage:       "",
	EtcdEndpoints: []string{"127.0.0.1:2379"},
}

// 加载配置文件
func LoadConfig() {
	defer func() {
		log.RecoverPanic(recover())
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
