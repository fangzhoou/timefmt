package core

import (
    "bufio"
    "bytes"
    "context"
    "encoding/gob"
    "errors"
    "fmt"
    "io"
    "os"
    "os/exec"
    "path"
    "strconv"
    "strings"
    "sync"

    "github.com/fangzhoou/dcron/utils"
)

const (
    // 任务状态：关闭 0
    JobStatusOff int = iota
    // 任务状态：启用 1
    JobStatusOn
    // 任务状态：删除 2
    JobStatusDisable

    // job 队列存储文件名
    JobStoreFileName = "jobqueue"
)

// 任务
type job struct {
    // 任务 id
    Id uint64

    // 任务名称
    Name string

    // 任务执行时间
    Spec string

    // 执行任务方式，shell、http-x
    Mode string

    // 执行语句
    Exec string

    // 任务描述
    Desc string

    // 任务执行参数
    Args map[string]interface{}

    // 任务依赖
    Depend []uint64

    // 执行机器数，单机或多机
    ExecNum int

    // 任务状态
    Status int

    mu sync.Mutex
}

// 创建 job 对象
func NewJob(id uint64, name, spec, mode, exec string) *job {
    return &job{
        Id:   id,
        Name: name,
        Spec: spec,
        Mode: mode,
        Exec: exec,
    }
}

// 执行任务
func (j *job) run() error {
    switch strings.ToLower(j.Mode) {
    case "shell":
        return j.runShell()
    case "http-get", "http-post", "http-put", "http-patch", "http-delete", "http-head", "http-options":
        return j.runHttp()
    default:
        return errors.New("job not support exec type: " + j.Mode)
    }
}

// 运行 shell 命令
func (j *job) runShell() error {
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
func (j *job) runHttp() error {
    return nil
}

// 任务队列
type jobQueue struct {
    // 任务队列，存放所有的任务
    Jobs []*job

    // 用于存放任务名称，添加任务时判断是否有重名的任务，重名任务不可添加
    JobsName map[string]int

    mu sync.Mutex
}

// 添加定时任务
func (jq *jobQueue) Add(ctx context.Context, j *job) error {
    // 查看任务队列里是否存在同名任务
    if _, ok := jq.JobsName[j.Name]; ok {
        return errors.New(fmt.Sprintf(`job name "%s" already existed`, j.Name))
    }

    // 校验定时任务语法是否正确
    if _, err := Parse(j.Spec); err != nil {
        return err
    }

    // 单机版创建新任务
    if Etcd.Cli == nil {
        id, err := jq.getSequenceId(ctx)
        if err != nil {
            return err
        }
        err = jq.addAndSave(ctx, id, j)
        if err != nil {
            return err
        }
        return nil
    }

    // 集群创建新任务
    // 利用 etcd 租约功能实现分布式锁
    em, err := NewEtcdMutex(getAddJobMutexKey())
    if err != nil {
        return err
    }
    em.Lock()
    defer em.Unlock()
    resp, err := Etcd.Cli.Get(ctx, getMaxJobIdKey())
    if err != nil {
        return err
    }
    if resp.Count > 0 {
        oid, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
        if err != nil {
            return err
        }

        // etcd 中的最大任务 id 不能小于当前任务队列的长度
        if oid < uint64(len(jq.Jobs)) {
            return errors.New("the max job id in etcd is less then local job queue length")
        }
        id := oid + 1
        err = jq.addAndSave(ctx, id, j)
        if err != nil {
            return err
        }
        return nil
    }

    return nil
}

// 添加 job 到队列 JobQueue，并保存到本地
func (jq *jobQueue) addAndSave(ctx context.Context, id uint64, j *job) error {
    job := NewJob(id, j.Name, j.Spec, j.Mode, j.Exec)
    job.Args = j.Args
    job.Desc = j.Desc

    // 序列化 job 对象到本地存储文件
    jq.mu.Lock()
    defer jq.mu.Unlock()
    err := job.serializeAndSave()
    if err != nil {
        return err
    }
    jq.Jobs = append(jq.Jobs, job)

    // 更新etcd job/max_id
    if Etcd.Cli != nil {
        _, err := Etcd.Cli.Put(ctx, getMaxJobIdKey(), strconv.FormatUint(id, 10))
        if err != nil {
            return err
        }
    }
    return nil
}

// 获取 job id
// id 等于已存在的最大任务 id + 1
// 集群的任务最大 id 存储在 etcd，key：Conf.name+"max_id"
// 单机则读取本地任务队列的长度加 1：len(JobQueue) + 1
func (jq *jobQueue) getSequenceId(ctx context.Context) (uint64, error) {
    var id uint64
    if Etcd.Cli == nil {
        id = uint64(len(jq.Jobs)) + 1
        return id, nil
    }

    // 从 etcd 中获取任务序列 id
    resp, err := Etcd.Cli.Get(ctx, getMaxJobIdKey())
    if err != nil {
        return id, err
    }
    if resp.Count > 0 {
        kv := resp.Kvs[0]
        id, err = strconv.ParseUint(string(kv.Value), 10, 64)
        if err != nil {
            return id, err
        }
    } else {
        id = 1
    }
    return id, nil
}

// 序列化并保存到本地
func (j *job) serializeAndSave() error {
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
    d := path.Join(rootPath, Conf.Name+"-data")
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
    fileName := path.Join(d, JobStoreFileName)
    _, err = os.Stat(fileName)
    if err != nil {
        if os.IsNotExist(err) {
            f, err := os.Create(fileName)
            if err != nil {
                return "", err
            }
            f.Close()
        } else {
            return "", err
        }
    }

    return fileName, nil
}

// 获取第 i 个任务
func (jq *jobQueue) GetJobById(i int) (j *job, err error) {
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
// 读取本地存储的 job 队列数据
func loadLocalJobs(ctx context.Context) error {
    log.Info("load local job queue...")
    fileName, err := getJobStoreFile()
    if err != nil {
        return err
    }
    file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDONLY, os.ModeAppend)
    defer file.Close()
    if err != nil {
        return err
    }
    info, err := file.Stat()
    if err != nil {
        return err
    }
    if info.Size() == 0 {
        log.Info("local job queue is empty")
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
        j := &job{}
        enc := gob.NewDecoder(bytes.NewBuffer(line))
        err = enc.Decode(j)
        if err != nil {
            return err
        }
        if j.Status != JobStatusDisable {
            JobQueue.mu.Lock()
            JobQueue.Jobs = append(JobQueue.Jobs, j)
            JobQueue.mu.Unlock()
        }
    }
    log.Info("load local job queue finished")

    // 同步 etcd 最大任务id
    err = updateEtcdMaxJobId(ctx, uint64(len(JobQueue.Jobs)))
    if err != nil {
        return err
    }
    return nil
}

// 更新 etcd 中保存的最大任务 id
func updateEtcdMaxJobId(ctx context.Context, id uint64) error {
    resp, err := Etcd.Cli.Get(ctx, getMaxJobIdKey())
    if err != nil {
        return err
    }
    if resp.Count > 0 {
        kv := resp.Kvs[0]
        oid, err := strconv.ParseUint(string(kv.Value), 10, 64)
        if err != nil {
            return err
        }
        if oid >= id {
            return nil
        }
    }
    _, err = Etcd.Cli.Put(ctx, getMaxJobIdKey(), strconv.FormatUint(id, 10))
    if err != nil {
        return err
    }
    return nil
}

// 获取 etcd 存储任务最大序列的 key
func getMaxJobIdKey() string {
    return Conf.Name + "/job/max_id"
}

// 获取添加任务时的 etcd 锁 key
func getAddJobMutexKey() string {
    return Conf.Name + "/job/add_lock"
}
