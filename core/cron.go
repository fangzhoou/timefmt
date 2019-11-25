package core

import (
	"container/heap"
	"context"
	"encoding/json"
	"time"

	"go.etcd.io/etcd/clientv3"
)

// 定时任务处理对象
// 管理、调度任务队列
type cron struct {
	// 定时任务待执行队列，小顶堆
	Entries *entryQueue

	Cancel context.CancelFunc
}

var Cron cron

func NewCron() *cron {
	Cron = cron{Entries: &entryQueue{}}
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
		NextTime: schedule.Next(time.Now()),
		Job:      j,
	}
	heap.Push(c.Entries, e)
	log.Debug("test add :", c.Entries)
	return nil
}

type runJob struct {
	runEntry
	Job *job
}

// 获取正在执行的任务
// 正在执行的任务是动态变化的，当前页显示有数据，可能下一秒就没有了
func (c *cron) FindEntries(page, size int) ([]*runJob, error) {
	list := make([]*runJob, 0)
	resp, err := Etcd().Cli.Get(context.TODO(), getRunEntryPrefix(), clientv3.WithPrefix())
	if err != nil {
		return list, err
	}

	for k, kv := range resp.Kvs {
		if k >= (page-1)*size && k < page*size {
			v := runEntry{}
			err = json.Unmarshal(kv.Value, &v)
			if err != nil {
				return list, err
			}
			id := 1
			j, err := JobQueue.FindJobById(id) // Cron.Entries[v.]
			if err != nil {
				return list, err
			}
			re := &runJob{v, j}
			list = append(list, re)
		}
	}
	return list, nil
}

// 启动定时任务
func (c *cron) Start() {
	log.Info(Conf.Name, "service starting...")
	var ctx context.Context
	ctx, c.Cancel = context.WithCancel(context.Background())

	// 初始化必要模块
	err := initModules(ctx)
	if err != nil {
		c.Cancel()
	}
	log.Info(Conf.Name, "service is working.")

	// 启动毫秒时钟
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			// 同一时间可能有多个任务需要执行
			for {
				if entry := c.Entries.First(); entry != nil && time.Now().After(entry.NextTime) {
					e := heap.Pop(c.Entries)
					e.(*Entry).Run(ctx)
					heap.Push(c.Entries, e)
				} else {
					break
				}
			}

		case <-ctx.Done():
			log.Fatal(Conf.Name, "service was stop:", ctx.Err())
			return
		}
	}
}

// 初始化必要组件
func initModules(ctx context.Context) error {
	// 服务注册与监控
	err := registerAndWatch(ctx)
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
