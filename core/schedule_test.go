package core

import (
    "container/heap"
    "fmt"
    "testing"
    "time"
)

func TestEntryHeap_Push(t *testing.T) {
    eh := &entryQueue{}
    job1 := NewJob(1, "111“”“", "*/3 * * * * *", "shell", "test1")
    schedule1, _ := Parse(job1.Spec)
    job2 := NewJob(2, "222“”“", "*/5 * * * * *", "shell", "test2")
    schedule2, _ := Parse(job2.Spec)
    job3 := NewJob(3, "333“”“", "*/10 * * * * *", "shell", "test3")
    schedule3, _ := Parse(job3.Spec)
    e1 := &Entry{Schedule: schedule1, NextTime: schedule1.Next(time.Now()), Job: job1}
    e2 := &Entry{Schedule: schedule2, NextTime: schedule2.Next(time.Now()), Job: job2}
    e3 := &Entry{Schedule: schedule3, NextTime: schedule3.Next(time.Now()), Job: job3}
    eh.Push(e1)
    eh.Push(e2)
    eh.Push(e3)
    log.Debug("eh:", eh)
    for {
        select {
        case <-time.After(300 * time.Millisecond):
            for {
                t := time.Now()
                if entry := eh.First(); entry != nil && t.After(entry.NextTime) {
                    e := heap.Pop(eh)
                    log.Debug("===", entry.NextTime, e.(*Entry).Job.Name, eh)
                    ee := *(e.(*Entry))
                    ee.PrevTime = time.Now()
                    ee.NextTime = e.(*Entry).Schedule.Next(time.Now())
                    log.Debug(ee.Job.Name, e.(*Entry).NextTime.String())
                    log.Debug(ee.Job.Name, ee.NextTime.String())
                    eh.Push(&ee)
                    //log.Debug(33, eh)
                } else {
                    break
                }
            }
        case <-time.After(1 * time.Minute):
            fmt.Println("end.")
        }
    }
}
