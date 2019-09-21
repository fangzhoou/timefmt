package core

import (
    "encoding/gob"
    "fmt"
    "os"
    "sync"

    "github.com/fangzhoou/dcron/utils"
)

// 任务
type job struct {
    // 任务名称
    Name string

    // 任务执行时间
    Spec string

    // 任务描述
    Desc string

    // 任务执行参数
    Args map[string]interface{}
}

// 任务队列
type jobQueue struct {
    // 任务队列
    Jobs []job

    mu sync.Mutex
}

// 添加定时任务
func (jq *jobQueue) Add(name, spec, desc string, args map[string]interface{}) error {
    j := job{
        Name: name,
        Spec: spec,
        Desc: desc,
        Args: args,
    }

    // 序列化 job 对象到本地存储文件
    err := serializeAndSave(j)
    if err != nil {
        return err
    }
    jq.Jobs = append(jq.Jobs, j)
    return nil
}

// 获取第 i 个任务
func (jq *jobQueue) GetJobById(i int) (j job, err error) {
    if len(jq.Jobs) == 0 {
        err = fmt.Errorf("job queue is empty")
        return
    }
    if i < 0 || i > len(jq.Jobs) {
        err = fmt.Errorf("job queue out of range")
        return
    }
    j = jq.Jobs[i-1]
    return
}

// 任务队列操作
var JobQueue *jobQueue

// 初始化 JobQueue
func InitJobQueue() {
    JobQueue = &jobQueue{}
}

// 序列化并保存到本地
func serializeAndSave(j job) error {
    fileName, err := getJobStoreFile()
    if err != nil {
        return err
    }
    f, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
    defer f.Close()
    if err != nil {
        return err
    }
    enc := gob.NewEncoder(f)
    err = enc.Encode(j)
    if err != nil {
        return err
    }
    return nil
}

// 获取 job 存储文件，文件不存在时创建
func getJobStoreFile() (string, error) {
    // 判断目录是否存在，不存在创建
    rootPath, err := utils.GetRootPath()
    if err != nil {
        return "", err
    }
    d := fmt.Sprintf("%s/%s", rootPath, Conf.Name+"-data")
    _, err = os.Stat(d)
    if err != nil {
        if os.IsNotExist(err) {
            err = os.MkdirAll(d, os.ModePerm)
            if err != nil {
                return "", err
            }
        } else {
            return "", err
        }
    }

    // 判断文件是否存在
    fileName := fmt.Sprintf("%s/%s", d, "jobqueue")
    _, err = os.Stat(fileName)
    if err != nil {
        if os.IsNotExist(err) {
            f, err := os.Create(fileName)
            if err != nil {
                return "", nil
            }
            f.Close()
        } else {
            return "", nil
        }
    }

    return fileName, nil
}
