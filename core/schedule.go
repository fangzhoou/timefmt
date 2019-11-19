package core

import (
    "container/heap"
    "context"
    "encoding/json"
    "fmt"
    "time"
)

const (
    // 任务状态：等待中
    StatusWaiting int = 1 << iota
    // 执行中
    StatusExecuting
)

// 时刻表接口
type Schedule interface {
    // 获取任务的下一次执行时间
    Next(time.Time) time.Time
}

// 单个任务执行对象
type Entry struct {
    // 时刻表
    Schedule Schedule

    // 上次执行时间
    PrevTime time.Time

    // 下次执行时间
    NextTime time.Time

    // 待执行的任务
    Job *job

    // 执行状态
    Status int

    // 堆中的索引位置
    index int

    // 任务是否结束
    fin chan bool
}

// 任务运行后存储在 etcd 中的数据
type runEntry struct {
    Ip       string
    PrevTime time.Time
    NextTime time.Time
    JobId    int
}

// 运行任务
// 运行的任务写入 etcd，通过 etcd 查询正在运行的任务
func (e *Entry) Run(ctx context.Context) {
    t := time.Now()
    e.Job.run(e.fin)
    e.PrevTime = t
    e.NextTime = e.Schedule.Next(t)
    e.Status = StatusExecuting

    // 写入 etcd
    values := &runEntry{
        Ip:       Server().IP,
        PrevTime: e.PrevTime,
        NextTime: e.NextTime,
        JobId:    e.Job.Id,
    }
    valueStr, err := json.Marshal(values)
    if err != nil {
        log.Error(err)
        Cron.Cancel()
    }
    _, err = Etcd().Cli.Put(ctx, getRunEntryKey(e.Job.Id), string(valueStr))
    if err != nil {
        log.Error(err)
        Cron.Cancel()
    }
    go func(e *Entry) {
        for {
            select {
            case b := <-e.fin:
                // 运行结束，删除 etcd 中的值
                if b {
                    e.Status = StatusWaiting
                    _, err := Etcd().Cli.Delete(ctx, getRunEntryKey(e.Job.Id))
                    if err != nil {
                        log.Error(err)
                        Cron.Cancel()
                    }
                }
            }
        }
    }(e)
}

// 执行中的任务 key
func getRunEntryKey(id int) string {
    return fmt.Sprintf("%s%d", getRunEntryPrefix(), id)
}

// 执行中的任务 key 前缀
func getRunEntryPrefix() string {
    return fmt.Sprintf("%s/entries/", Conf.Name)
}

// 任务清单队列，小顶堆，实现 container/heap
type entryQueue []*Entry

func (q entryQueue) Len() int { return len(q) }

func (q entryQueue) Less(i, j int) bool {
    return q[i].NextTime.UnixNano() < q[j].NextTime.UnixNano()
}

func (q entryQueue) Swap(i, j int) {
    q[i], q[j] = q[j], q[i]
    q[i].index = i
    q[j].index = j
}

// 添加元素
func (q *entryQueue) Push(x interface{}) {
    // 使用指针（*h）操作堆，因为操作需要反应到原切片里
    n := len(*q)
    item := x.(*Entry)
    item.index = n
    *q = append(*q, item)
}

// 移除首个元素
func (q *entryQueue) Pop() interface{} {
    old := *q
    n := len(old)
    item := old[0]
    item.index = -1
    *q = old[1:n]
    return item
}

// 获取堆中首个元素
// entryHeap.First()
func (q *entryQueue) First() *Entry {
    if len(*q) > 0 {
        e := q.Pop()
        q.Push(e)
        return e.(*Entry)
    }
    return nil
}

// 更新元素
func (q *entryQueue) Update(item *Entry, nextTime time.Time, job *job) {
    item.NextTime = nextTime
    item.Job = job
    heap.Fix(q, item.index)
}

// 删除指定元素
// entryHeap.Delete(entry)
func (q *entryQueue) Delete(item Entry) bool {
    heap.Remove(q, item.index)
    return true
}
