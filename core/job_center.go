package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
)

const (
	// 任务状态：关闭
	JobStatusOff int = 1 << iota
	// 任务状态：启用
	JobStatusOn

	// job 队列存储文件名
	JobStoreFileName string = "job_queue"

	// job 更新内容存储文件名
	JobUpdateFileName string = "job_update"

	// job 序列化存储的字符串分割符
	JobSeparator byte = '|'

	// MaxJobIdKey 租约时间，默认3秒
	MaxJobIdLeaseTime = 3
)

// 任务
type job struct {
	// 任务 id
	Id int

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
	Depend []int

	// 执行机器数，单机或多机
	ExecNum int

	// 任务状态
	Status int

	mu sync.Mutex
}

// 创建 job 对象
func NewJob(id int, p map[string]interface{}) *job {
	job := &job{
		Id:      id,
		Name:    p["name"].(string),
		Spec:    p["spec"].(string),
		Mode:    p["mode"].(string),
		Exec:    p["exec"].(string),
		Depend:  make([]int, 0),
		ExecNum: 1,
		Status:  JobStatusOff, // 默认关闭状态
	}
	if desc := p["desc"].(string); desc != "" {
		job.Desc = p["desc"].(string)
	}
	if args := p["args"].(map[string]interface{}); args != nil {
		job.Args = args
	}
	if dep := p["depend"].([]int); len(dep) > 0 {
		job.Depend = dep
	}
	if en := p["exec_num"].(int); en > 0 {
		job.ExecNum = en
	}
	return job
}

// 执行任务
func (j *job) run(fin chan bool) {
	go func(fin chan bool) {
		var err error
		switch strings.ToLower(j.Mode) {
		case "shell":
			err = j.runShell(fin)
		case "http-get", "http-post", "http-put", "http-patch", "http-delete", "http-head", "http-options":
			err = j.runHttp(fin)
		default:
			err = fmt.Errorf("job not support exec type:%s", j.Mode)
		}
		if err != nil {
			log.Error(err)
			fin <- true
		}
	}(fin)
}

// 运行 shell 命令
func (j *job) runShell(fin chan bool) error {
	cmd := exec.Command(`/bin/bash`, "-c", j.Exec)
	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	outBytes, err := ioutil.ReadAll(stdOut)
	log.Info(j.Name, string(outBytes))
	if err := cmd.Wait(); err != nil {
		return err
	}
	fin <- true
	return nil
}

// 执行 http 请求
func (j *job) runHttp(fin chan bool) error {
	return nil
}

// 任务队列，存放全量任务
type jobQueue struct {
	// 任务队列
	Jobs []*job

	// 用于存放任务名称，添加任务时判断是否有重名的任务，重名任务不可添加
	JobsName map[string]int

	mu sync.Mutex
}

// 添加定时任务
func (jq *jobQueue) Add(ctx context.Context, p map[string]interface{}) error {
	// 查看任务队列里是否存在同名任务
	if _, ok := jq.JobsName[p["name"].(string)]; ok {
		return errors.New(fmt.Sprintf(`job name "%v" already existed`, p["name"]))
	}

	// 利用 etcd 租约功能实现分布式锁
	em, err := NewEtcdMutex(getAddJobMutexKey())
	if err != nil {
		return err
	}
	em.Lock()
	id, err := jq.getSequenceId(ctx)
	if err != nil {
		return err
	}
	job := NewJob(id, p)
	jobByte, err := json.Marshal(job)
	if err != nil {
		return errors.New("json marshal job failed")
	}

	// 更新任务最大id
	_, err = Etcd().Cli.Put(ctx, getMaxJobIdKey(), strconv.Itoa(id))
	if err != nil {
		return err
	}

	// 更新待同步任务列表，新增记录
	// dcron/new_job_list/{id} => string(job)
	_, err = Etcd().Cli.Put(ctx, getNewJobKey(id), string(jobByte))
	if err != nil {
		return err
	}

	// 保存到本地
	// 保存本地操作在同步 etcd 成功后操作，这样可以避免当前结点同步 etcd 失败后，
	// 再重启服务结点出现数据不同步问题。
	err = jq.addAndSave(ctx, job)
	if err != nil {
		return err
	}
	em.Unlock()

	// 同步更新其它结点数据
	go syncJobToOtherNode(ctx, id, jobByte)

	return nil
}

// 更新其它结点数据
func syncJobToOtherNode(ctx context.Context, jobId int, jobByte []byte) {
	syncRe := make(chan bool, 1)
	var n int
	for _, node := range Master().NodeList {
		if node.Key != getNodePrefix()+Server().Name {
			n++
			url := fmt.Sprintf("http://%s:%d/job/sync", node.Ip, Conf.Port)
			body := bytes.NewReader(jobByte)
			go func(c chan<- bool) {
				_, err := http.Post(url, "text/html", body)
				if err != nil {
					c <- false
				} else {
					c <- true
				}
			}(syncRe)
		}
	}

	i := 0
	for {
		select {
		case b := <-syncRe:
			if b {
				i++
			}
		case <-time.After(5 * time.Second):
			break
		}
	}

	// 所有结点同步成功，删除对应的任务数据
	if i == n {
		Etcd().Cli.Delete(ctx, getNewJobKey(jobId))
	}
}

// 添加 job 到队列 JobQueue，并保存到本地
func (jq *jobQueue) addAndSave(ctx context.Context, j *job) error {
	// 序列化 job 对象到本地存储文件
	jq.mu.Lock()
	defer jq.mu.Unlock()
	err := j.serializeAndSave(JobStoreFileName)
	if err != nil {
		return err
	}
	// 任务按 id 由小到大排序
	for i := len(jq.Jobs) - 1; i >= 0; i-- {
		if jq.Jobs[i].Id < j.Id {
			new := make([]*job, len(jq.Jobs[i+1:]))
			copy(new, jq.Jobs[i+1:])
			jq.Jobs = append(append(jq.Jobs[:i+1], j), new...)
			break
		}
	}
	jq.JobsName[j.Name] = 1
	return nil
}

// 获取 job id
// id 等于已存在的最大任务 id + 1
// 集群的任务最大 id 存储在 etcd，key：Conf.name+"max_id"
func (jq *jobQueue) getSequenceId(ctx context.Context) (int, error) {
	var id int
	resp, err := Etcd().Cli.Get(ctx, getMaxJobIdKey())
	if err != nil {
		return id, err
	}
	if resp.Count > 0 {
		kv := resp.Kvs[0]
		id, err = strconv.Atoi(string(kv.Value))
		if err != nil {
			return id, err
		}
		id += 1
	} else {
		id = 1
	}
	return id, nil
}

// 序列化并保存到本地
func (j *job) serializeAndSave(fName string) error {
	fileName, err := getStoreFile(fName)
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
	buf.WriteByte(JobSeparator) // 末尾追加分割符
	_, err = f.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

// 获取 job 存储文件，文件不存在时创建
func getStoreFile(fName string) (string, error) {
	// 判断目录是否存在，不存在创建
	var rootPath string
	var err error
	if Conf.Storage != "" {
		rootPath = Conf.Storage
	} else {
		rootPath, err = GetRootPath()
		if err != nil {
			return "", err
		}
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
	fileName := path.Join(d, fName)
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

// 同步数据
func (jq *jobQueue) SyncJob(ctx context.Context, data []byte) error {
	j := &job{}
	err := json.Unmarshal(data, j)
	if err != nil {
		return err
	}
	return jq.addAndSave(ctx, j)
}

// 更新单个任务
// 除 Id、名称外，其它属性都可更新
func (jq *jobQueue) UpdateById(id int, p map[string]interface{}) (*job, error) {
	job, err := jq.FindJobById(id)
	if err != nil {
		return nil, err
	}
	jq.mu.Lock()
	defer jq.mu.Unlock()

	// 有值更新，Job 为指针更新会反映到 jobQueue 中
	if name, ok := p["name"]; ok && name.(string) != "" {
		job.Name = name.(string)
	}
	if spec, ok := p["spec"]; ok && spec.(string) != "" {
		job.Spec = spec.(string)
	}
	if mode, ok := p["mode"]; ok && mode.(string) != "" {
		job.Mode = mode.(string)
	}
	if exec, ok := p["exec"]; ok && exec.(string) != "" {
		job.Exec = exec.(string)
	}
	if desc, ok := p["desc"]; ok && desc.(string) != "" {
		job.Desc = desc.(string)
	}
	if args, ok := p["args"]; ok && args.(map[string]interface{}) != nil {
		job.Args = args.(map[string]interface{})
	}
	if depend, ok := p["depend"]; ok && len(depend.([]int)) > 0 {
		job.Depend = depend.([]int)
	}
	if execNum, ok := p["exec_num"]; ok && execNum.(int) > 0 && execNum.(int) != job.ExecNum {
		job.ExecNum = execNum.(int)
	}
	if status, ok := p["status"]; ok && status.(int) != job.Status {
		job.Status = status.(int)

		// 处理任务状态
		err = handleJobStatusById(job)
		if err != nil {
			return nil, err
		}
	}

	// 更新内容缓存到本地文件
	// 另外的线程会去消费这个内容，并同步到全量任务文件中
	// 更新完成后，更新内容会被删除，控制文件大小
	err = job.serializeAndSave(JobUpdateFileName)
	if err != nil {
		return job, err
	}
	return job, nil
}

// 处理任务状态
func handleJobStatusById(job *job) error {
	// 启用时添加到执行队列
	if job.Status == JobStatusOn {
		return Cron.AddJob(job)
	}

	// 暂停或删除时从执行队列中移除包含 job 的执行任务
	if job.Status == JobStatusOff {
		if b := Cron.Entries.Delete(Entry{Job: job}); b {
			return nil
		} else {
			return fmt.Errorf("delete exec entry job failed, job id:%d", job.Id)
		}
	}
	return nil
}

// 获取第 i 个任务
func (jq *jobQueue) FindJobById(i int) (j *job, err error) {
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

// 获取 job 列表
func (jq *jobQueue) FindJobList(page, size int) ([]*job, error) {
	list := make([]*job, 0)
	totalPage := int(math.Ceil(float64(len(jq.Jobs)) / float64(size)))
	if page > totalPage {
		return nil, fmt.Errorf("find jobs out of range")
	}
	if page == totalPage {
		list = jq.Jobs[(page-1)*size:]
	} else {
		list = jq.Jobs[(page-1)*size : page*size+1]
	}
	return list, nil
}

// 根据 id 删除 job
func (jq *jobQueue) DeleteById(id int) error {
	if id < 0 || id > len(jq.Jobs) {
		return fmt.Errorf("id:%d is out of range", id)
	}
	newJobs := append(jq.Jobs[:id-1], jq.Jobs[id:]...)
	jq.Jobs = newJobs
	return nil
}

// 任务队列操作
var JobQueue = &jobQueue{
	Jobs:     make([]*job, 0),
	JobsName: map[string]int{},
}

// 初始化 JobQueue
// 读取本地存储的 job 队列数据
func loadLocalJobs(ctx context.Context) error {
	log.Info("load local job queue...")
	fileName, err := getStoreFile(JobStoreFileName)
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
		line, err := buf.ReadSlice(JobSeparator)
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
		JobQueue.mu.Lock()
		JobQueue.Jobs = append(JobQueue.Jobs, j)
		JobQueue.JobsName[j.Name] = 1
		JobQueue.mu.Unlock()
	}
	log.Info("load local job queue finished")

	// 同步 etcd 最大任务id
	var id int
	if l := len(JobQueue.Jobs); l > 0 {
		maxJob := JobQueue.Jobs[l-1]
		id = maxJob.Id
	}
	err = updateEtcdMaxJobId(ctx, id)
	if err != nil {
		return err
	}
	return nil
}

// 更新 etcd 中保存的最大任务 id
func updateEtcdMaxJobId(ctx context.Context, id int) error {
	em, err := NewEtcdMutex("update_max_job_id")
	if err != nil {
		return err
	}
	em.Lock()
	defer em.Unlock()

	resp, err := Etcd().Cli.Get(ctx, getMaxJobIdKey())
	if err != nil {
		return err
	}
	if resp.Count > 0 {
		kv := resp.Kvs[0]
		eid, err := strconv.Atoi(string(kv.Value))
		if err != nil {
			return err
		}
		if eid >= id {
			return nil
		}
	}

	// 更新最大 maxJobId 时需要创建租约，如果其它结点更新最大值时方便释放本机的租约
	// 保证最大值 maxJobId 只有一台机器维护
	lResp, err := Etcd().Cli.Grant(ctx, MaxJobIdLeaseTime)
	if err != nil {
		return err
	}
	_, err = Etcd().Cli.Put(ctx, getMaxJobIdKey(), strconv.Itoa(id), clientv3.WithLease(lResp.ID))
	if err != nil {
		return err
	}
	ka, err := Etcd().Cli.KeepAlive(ctx, lResp.ID)
	if err != nil {
		return err
	}
	go func(ctx context.Context, id clientv3.LeaseID) {
		for {
			select {
			case re := <-ka:
				if re == nil {
					_, err := Etcd().Cli.Revoke(ctx, lResp.ID)
					if err != nil {
						log.Error(err)
						Cron.Cancel()
					}
				}
			}
		}
	}(ctx, lResp.ID)
	go watchMaxJobId(ctx, lResp.ID) // 监控 maxJobIdKey 值
	return nil
}

// 监控 MaxJobIdKey 的值
// 值有变化终止本机对 MaxJobIdKey 租约
func watchMaxJobId(ctx context.Context, id clientv3.LeaseID) {
	c := Etcd().Cli.Watch(ctx, getMaxJobIdKey())
	for {
		select {
		case resp := <-c:
			if resp.Err() != nil {
				log.Error(resp.Err())
				_, err := Etcd().Cli.Revoke(ctx, id)
				if err != nil {
					log.Error(err)
					Cron.Cancel()
				}
			}
			for _, ev := range resp.Events {
				switch ev.Type {

				// 只有改变时，释放租约
				case mvccpb.PUT, mvccpb.DELETE:
					_, err := Etcd().Cli.Revoke(ctx, id)
					if err != nil {
						log.Error(err)
						Cron.Cancel()
					}
				}
			}
		}
	}
}

// 获取 etcd 存储任务最大序列的 key
func getMaxJobIdKey() string {
	return Conf.Name + "/job/max_id"
}

// 获取添加任务时的 etcd 锁 key
func getAddJobMutexKey() string {
	return Conf.Name + "/job/add_lock"
}

// 获取新增任务列表 key
func getNewJobListKey() string {
	return Conf.Name + "/new_job_list"
}

// 获取新增的单个任务key
func getNewJobKey(jobId int) string {
	return getNewJobListKey() + "/" + strconv.Itoa(jobId)
}
