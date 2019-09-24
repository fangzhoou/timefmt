package main

import (
    "github.com/fangzhoou/dcron/core"
)

func main() {
    core.LoadConfig()
    core.NewCron().Start()
}
