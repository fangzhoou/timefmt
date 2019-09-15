package main

import (
    "fmt"
    "time"

    "github.com/fangzhoou/dcron/core"
)

func init() {
    core.LoadConfig()
}

func main() {
    cron := core.NewCron()
    fmt.Println(1111)

    cron.AddJob("*/5 * * * * *", func() {
        fmt.Println(time.Now().String(), ":  this is a test job 11111")
    })
    fmt.Println(2222)

    cron.AddJob("*/10 * * * * *", func() {
        fmt.Println(time.Now().String(), ":  this is a test job 22222")
    })
    fmt.Println(3333)

    cron.Start()
}
