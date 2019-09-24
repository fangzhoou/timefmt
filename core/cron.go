package core

import (
    "container/heap"
    "fmt"
    "time"
)

// 定时任务处理对象
// 管理、调用定时任务的方法
type cron struct {
    // 定时任务待执行队列，小顶堆
    Entries *entryHeap
}

var Cron cron

func NewCron() *cron {
    Cron = cron{Entries: &entryHeap{}}
    return &Cron
}

// 添加定时任务
func (c *cron) AddJob(j *Job) error {
    schedule, err := Parse(j.Spec)
    if err != nil {
        return err
    }
    e := &Entry{
        Schedule: schedule,
        PrevTime: time.Time{},
        NextTime: schedule.Next(time.Now()),
        Job:      j,
    }
    heap.Push(c.Entries, e)
    c.Entries.Push(e)
    return nil
}

// 启动定时任务
func (c *cron) Start() {
    fmt.Println(Conf.Name + " start...")
    // 初始化必要模块
    initModules()

    ticker := time.NewTicker(time.Millisecond)
    fmt.Println(Conf.Name + " is working.")
    for {
        select {
        case <-ticker.C:
            // 任务队列为小顶堆，每次都读取根结点，比较下次执行时间
            entry := c.Entries.First()
            t := time.Now()
            if entry != nil && t.After(entry.NextTime) {
                e := heap.Pop(c.Entries)
                fmt.Println("aaaaa")
                e.(*Entry).Job.run()
                e.(*Entry).PrevTime = t
                e.(*Entry).NextTime = e.(*Entry).Schedule.Next(t)
                c.Entries.Push(e)
            }
        }
    }
}

// 初始化必要组件
func initModules() {
    // 初始化 etcd
    InitEtcd()

    // 初始化任务队列
    err := InitJobQueue()
    if err != nil {
        panic(err)
    }

    // 监听 http 服务
    go ListenAndServe()
}
