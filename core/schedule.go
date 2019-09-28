package core

import (
    "time"
)

const (
    // 任务状态：等待中
    StatusWaiting int = 1 << iota
    // 执行中
    StatusExecuting
    // 无效
    StatusInvalid
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
}

// 任务清单队列，小顶堆，实现 container/heap
type entryHeap []*Entry

func (h entryHeap) Len() int { return len(h) }

func (h entryHeap) Less(i, j int) bool {
    return h[i].NextTime.UnixNano() < h[j].NextTime.UnixNano()
}

func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// 添加元素
func (h *entryHeap) Push(x interface{}) {
    // 使用指针操作堆，因为操作需要反应到原切片里
    *h = append(*h, x.(*Entry))
}

// 移除首个元素
func (h *entryHeap) Pop() interface{} {
    x := (*h)[0]
    n := len(*h)
    *h = (*h)[1:n]
    return x
}

// 获取堆中首个元素
func (h *entryHeap) First() *Entry {
    if len(*h) > 0 {
        return (*h)[0]
    }
    return nil
}
