package main

import (
    "github.com/fangzhoou/dcron/core"
)

func init() {
    core.LoadConfig()
}

func main() {
    core.NewCron().Start()
}
