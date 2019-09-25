package core

import (
    "container/heap"
    "fmt"
    "time"
)

type StopChan chan bool

func (sc StopChan) Done() {
    sc <- true
}

// 定时任务处理对象
// 管理、调度任务队列
type cron struct {
    // 定时任务待执行队列，小顶堆
    Entries *entryHeap

    Stop StopChan
}

var Cron cron

func NewCron() *cron {
    Cron = cron{Entries: &entryHeap{}, Stop: make(StopChan)}
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
    fmt.Println(Conf.Name + " service is starting...")

    // 初始化必要模块
    c.initModules()

    fmt.Println(Conf.Name + " service is working.")
    // 启动毫秒时钟
    for {
        select {
        case <-time.After(100 * time.Millisecond):
            // 任务队列为小顶堆，每次都读取根结点，比较下次执行时间
            entry := c.Entries.First()
            t := time.Now()
            if entry != nil && t.After(entry.NextTime) {
                e := heap.Pop(c.Entries)
                e.(*Entry).Job.run()
                e.(*Entry).PrevTime = t
                e.(*Entry).NextTime = e.(*Entry).Schedule.Next(t)
                c.Entries.Push(e)
            }
        case <-c.Stop:
            panic(Conf.Name + " service was stop")
        }
    }
}

// 初始化必要组件
func (c *cron) initModules() {
    fmt.Println("init modules...")
    // 初始化 etcd
    InitEtcd(c.Stop)

    // 初始化任务队列
    err := InitJobQueue()
    if err != nil {
        panic(err)
    }

    // 监听 http 服务
    go ListenAndServe()
}
