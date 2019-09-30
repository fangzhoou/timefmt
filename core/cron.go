package core

import (
    "container/heap"
    "context"
    "time"
)

// 定时任务处理对象
// 管理、调度任务队列
type cron struct {
    // 定时任务待执行队列，小顶堆
    Entries *entryHeap

    Cancel context.CancelFunc
}

var Cron cron

func NewCron() *cron {
    Cron = cron{Entries: &entryHeap{}}
    return &Cron
}

// 添加定时任务
func (c *cron) AddJob(j *job) error {
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
    return nil
}

// 启动定时任务
func (c *cron) Start() {
    log.Info(Conf.Name, "service starting...")
    ctx, cancel := context.WithCancel(context.Background())
    c.Cancel = cancel

    // 初始化必要模块
    err := c.initModules(ctx)
    if err != nil {
        log.Error(err)
        c.Cancel()
    }
    log.Info(Conf.Name, "service is working.")

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
        case <-ctx.Done():
            log.Fatal(Conf.Name, "service was stop: ", ctx.Err())
            return
        }
    }
}

// 初始化必要组件
func (c *cron) initModules(ctx context.Context) error {
    // 初始化 etcd
    err := InitEtcd(ctx)
    if err != nil {
        return err
    }

    // 加载本地任务队列
    err = loadLocalJobs(ctx)
    if err != nil {
        return err
    }

    // 监听 http 服务
    ListenAndServe()
    return nil
}
