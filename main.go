package main

import (
    "github.com/fangzhoou/dcron/core"
)

func init() {
    core.LoadConfig()
    core.InitEtcd()
}

func main() {
    core.NewCron().Start()
}
