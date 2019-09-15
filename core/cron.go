package core

import (
    "container/heap"
    "time"
)

// 定时任务处理对象
// 管理、调用定时任务的方法
type Cron struct {
    // 定时任务待执行队列，小顶堆
    Entries *entryHeap
}

func NewCron() *Cron {
    return &Cron{Entries: &entryHeap{}}
}

// 添加定时任务
func (c *Cron) AddJob(spec string, fn func()) error {
    schedule, err := Parse(spec)
    if err != nil {
        return err
    }
    e := &Entry{
        schedule,
        time.Time{},
        schedule.Next(time.Now()),
        funcJob(fn),
    }
    heap.Push(c.Entries, e)
    c.Entries.Push(e)
    return nil
}

// 启动定时任务
func (c *Cron) Start() {
    // 添加定时任务结点
    go addCronNode()

    ticker := time.NewTicker(time.Millisecond)
    for {
        select {
        case <-ticker.C:
            // 任务队列为小顶堆，每次都读取根结点，比较下次执行时间
            e := c.Entries.First()
            if t := time.Now(); t.UnixNano() > e.NextTime.UnixNano() {
                execE := heap.Pop(c.Entries)
                execE.(*Entry).Job.run()
                execE.(*Entry).PrevTime = t
                execE.(*Entry).NextTime = execE.(*Entry).Schedule.Next(t)
                c.Entries.Push(execE)
            }
        }
    }
}
