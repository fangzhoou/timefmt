package core

import (
    "bufio"
    "bytes"
    "encoding/gob"
    "errors"
    "fmt"
    "io"
    "os"
    "os/exec"
    "strings"
    "sync"

    "github.com/fangzhoou/dcron/utils"
)

const (
    // 任务状态：关闭
    JobStatusOff = 0
    // 任务状态：启用
    JobStatusOn = 1

    // job 队列存储文件名
    JobStoreFileName = "jobqueue"
)

// 任务
type Job struct {
    // 任务 id
    Id uint64

    // 任务名称
    Name string

    // 任务执行时间
    Spec string

    // 执行任务方式
    Type string

    // 执行语句
    Exec string

    // 任务描述
    Desc string

    // 任务执行参数
    Args map[string]interface{}

    // 任务状态
    Status int
}

// 执行任务
func (j *Job) run() error {
    switch strings.ToLower(j.Type) {
    case "shell":
        return j.runShell()
    case "http-get", "http-post", "http-put", "http-patch", "http-delete", "http-head", "http-options":
        return j.runHttp()
    case "rpc":
        return j.runRpc()
    default:
        return errors.New("job not support exec type: " + j.Type)
    }
}

// 运行 shell 命令
func (j *Job) runShell() error {
    cmd := exec.Command(`/bin/bash`, "-c", j.Exec)
    var out bytes.Buffer
    cmd.Stdout = &out
    err := cmd.Run()
    if err != nil {
        return fmt.Errorf(err.Error())
    }
    fmt.Print(out.String())
    return err
}

// 执行 http 请求
func (j *Job) runHttp() error {
    return nil
}

// 执行 rpc 请求
func (j *Job) runRpc() error {
    return nil
}

// 任务队列
type jobQueue struct {
    Jobs []Job

    mu sync.Mutex
}

// 添加定时任务
func (jq *jobQueue) Add(j *Job) error {
    // 查看任务队列里是否存在同名任务
    for _, job := range jq.Jobs {
        if job.Name == j.Name {
            return errors.New(fmt.Sprintf(`job "%s" already existed`, j.Name))
        }
    }

    // 序列化 job 对象到本地存储文件
    jq.mu.Lock()
    err := j.serializeAndSave()
    if err != nil {
        return err
    }
    jq.Jobs = append(jq.Jobs, *j)
    jq.mu.Unlock()
    return nil
}

// 序列化并保存到本地
func (j *Job) serializeAndSave() error {
    fileName, err := getJobStoreFile()
    if err != nil {
        return err
    }
    f, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
    defer f.Close()
    if err != nil {
        return err
    }

    // 先把 Job 对象数据写入缓冲中，再写入文件，末尾换行
    buf := &bytes.Buffer{}
    enc := gob.NewEncoder(buf)
    err = enc.Encode(j)
    if err != nil {
        return err
    }
    buf.WriteByte('\n') // 末尾追加换行
    _, err = f.Write(buf.Bytes())
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

    // 判断文件是否存在，不存在则创建
    fileName := fmt.Sprintf("%s/%s", d, JobStoreFileName)
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

// 获取第 i 个任务
func (jq *jobQueue) GetJobById(i int) (j Job, err error) {
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
var JobQueue = &jobQueue{}

// 初始化 JobQueue
func InitJobQueue() error {
    // 读取本地存储的 job
    JobQueue.mu.Lock()
    defer JobQueue.mu.Unlock()
    fileName, err := getJobStoreFile()
    if err != nil {
        return err
    }
    file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDONLY, os.ModeAppend)
    defer file.Close()
    if err != nil {
        return err
    }
    fi, err := file.Stat()
    if err != nil {
        return err
    }
    if fi.Size() == 0 {
        return nil
    }

    buf := bufio.NewReader(file)
    for {
        line, err := buf.ReadBytes('\n')
        if err != nil {
            if err == io.EOF {
                break
            } else {
                return err
            }
        }
        b := bytes.NewBuffer(line)
        j := Job{}
        enc := gob.NewDecoder(b)
        err = enc.Decode(&j)
        if err != nil {
            return err
        }
        JobQueue.Jobs = append(JobQueue.Jobs, j)
    }
    return nil
}
