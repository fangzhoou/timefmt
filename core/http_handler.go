package core

import (
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
)

// 监听 http 服务
func ListenAndServe() {
	router := fasthttprouter.New()
	setRoutes(router)
	port := strconv.Itoa(Conf.Port)
	go fasthttp.ListenAndServe(":"+port, router.Handler)
	log.Info("listening for client requests on localhost:" + port)
}

// 设置路由
func setRoutes(r *fasthttprouter.Router) {
	r.POST("/job", addJob)             // 添加任务
	r.PUT("/job", addJob)              // 添加任务
	r.PATCH("/job/:id", updateJob)     // 修改任务
	r.GET("/jobs", findJobs)           // 查询任务列表
	r.GET("/job/:id", findJobById)     // 查询单个任务
	r.DELETE("/job/:id", deleteById)   // 删除单个任务
	r.PUT("/job/:id/on", setJobOn)     // 启动任务
	r.PUT("/job/:id/off", setJobOff)   // 关闭任务
	r.GET("/entries", findExecEntries) // 获取正在执行的任务

	r.POST("/job/sync", syncData) // 内容结点同步数据接口，禁止外部调用
}

// 添加 job
// @param name string 任务名称
// @param spec string 时间描述
// @param mode string 执行方式
// @param exec string 执行语句
// @param args map[string]interface{} 附带参数
// @param depend []uint64 依赖的任务 id
// @param exec_num int 执行机器数
// @param desc string 任务描述
func addJob(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html")
	postArgs := ctx.PostArgs()
	name := string(postArgs.Peek("name"))
	spec := string(postArgs.Peek("spec"))
	mode := string(postArgs.Peek("mode"))
	exec := string(postArgs.Peek("exec"))
	args := postArgs.Peek("args")
	depend := postArgs.Peek("depend")
	execNum := string(postArgs.Peek("exec_num"))
	desc := string(postArgs.Peek("desc"))

	// 校验参数
	if name == "" {
		send(ctx, 400, "param name is required", nil)
		return
	}
	if spec == "" {
		send(ctx, 400, "param spec is required", nil)
		return
	}
	if mode == "" {
		send(ctx, 400, "param mode is required", nil)
		return
	}
	if exec == "" {
		send(ctx, 400, "param exec is required", nil)
		return
	}
	var jobArgs map[string]interface{}
	if string(args) != "" {
		err := json.Unmarshal(args, &jobArgs)
		if err != nil {
			send(ctx, 400, "param args format error", nil)
			return
		}
	}
	var jobDepend []int
	if string(depend) != "" {
		err := json.Unmarshal(depend, &jobDepend)
		if err != nil {
			send(ctx, 400, "param depend format error", nil)
			return
		}
	}
	var jobExecNum int
	var err error
	if execNum != "" {
		jobExecNum, err = strconv.Atoi(execNum)
		if err != nil {
			send(ctx, 400, "param depend format error", nil)
			return
		}
	}

	values := map[string]interface{}{
		"name":     name,
		"spec":     spec,
		"mode":     mode,
		"exec":     exec,
		"args":     jobArgs,
		"depend":   jobDepend,
		"exec_num": jobExecNum,
		"desc":     desc,
	}
	err = checkJobFields(values)
	if err != nil {
		send(ctx, 400, err.Error(), nil)
		return
	}

	err = JobQueue.Add(ctx, values)
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	send(ctx, 200, "ok", nil)
	return
}

// 修改任务
func updateJob(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html")
	id, err := strconv.Atoi(ctx.UserValue("id").(string))
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}

	// 更新的参数
	postArgs := ctx.PostArgs()
	name := string(postArgs.Peek("name"))
	spec := string(postArgs.Peek("spec"))
	mode := string(postArgs.Peek("mode"))
	exec := string(postArgs.Peek("exec"))
	args := postArgs.Peek("args")
	depend := postArgs.Peek("depend")
	execNum := string(postArgs.Peek("exec_num"))
	desc := string(postArgs.Peek("desc"))
	var jobArgs map[string]interface{}
	if string(args) != "" {
		err := json.Unmarshal(args, &jobArgs)
		if err != nil {
			send(ctx, 400, "param args format error", nil)
			return
		}
	}
	var jobDepend []int
	if string(depend) != "" {
		err := json.Unmarshal(depend, &jobDepend)
		if err != nil {
			send(ctx, 400, "param depend format error", nil)
			return
		}
	}
	var jobExecNum int
	if execNum != "" {
		jobExecNum, err = strconv.Atoi(execNum)
		if err != nil {
			send(ctx, 400, "param depend format error", nil)
			return
		}
	}

	values := map[string]interface{}{
		"name":     name,
		"spec":     spec,
		"mode":     mode,
		"exec":     exec,
		"args":     jobArgs,
		"depend":   jobDepend,
		"exec_num": jobExecNum,
		"desc":     desc,
	}
	err = checkJobFields(values)
	if err != nil {
		send(ctx, 400, err.Error(), nil)
		return
	}
	job, err := JobQueue.UpdateById(id, values)
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	send(ctx, 200, "ok", job)
	return
}

// 查询任务列表
func findJobs(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html")
	var page, size int
	var err error
	queryArgs := ctx.QueryArgs()
	p := string(queryArgs.Peek("page"))
	s := string(queryArgs.Peek("size"))
	if p != "" {
		page, err = strconv.Atoi(p)
		if err != nil {
			send(ctx, 500, err.Error(), nil)
			return
		}
	}
	if s != "" {
		size, err = strconv.Atoi(s)
		if err != nil {
			send(ctx, 500, err.Error(), nil)
			return
		}
	}
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 1
	}
	result, err := JobQueue.FindJobList(page, size)
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	send(ctx, 200, "", result)
	return
}

// 查询单个任务
func findJobById(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html")
	id, err := strconv.Atoi(ctx.UserValue("id").(string))
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	job, err := JobQueue.FindJobById(id)
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	send(ctx, 200, "", job)
	return
}

// 删除单个任务
func deleteById(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html")
	id, err := strconv.Atoi(ctx.UserValue("id").(string))
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	err = JobQueue.DeleteById(id)
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	send(ctx, 200, "ok", nil)
	return
}

// 启动任务
func setJobOn(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html")
	id, err := strconv.Atoi(ctx.UserValue("id").(string))
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	values := map[string]interface{}{
		"status": JobStatusOn,
	}
	job, err := JobQueue.UpdateById(id, values)
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	send(ctx, 200, "ok", job)
	return
}

// 关闭任务
func setJobOff(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html")
	id, err := strconv.Atoi(ctx.UserValue("id").(string))
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	values := map[string]interface{}{
		"status": JobStatusOff,
	}
	job, err := JobQueue.UpdateById(id, values)
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	send(ctx, 200, "ok", job)
	return
}

// 获取正在执行的任务列表
func findExecEntries(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html")
	var page, size int
	var err error
	queryArgs := ctx.QueryArgs()
	p := string(queryArgs.Peek("page"))
	s := string(queryArgs.Peek("size"))
	if p != "" {
		page, err = strconv.Atoi(p)
		if err != nil {
			send(ctx, 500, err.Error(), nil)
			return
		}
	}
	if s != "" {
		size, err = strconv.Atoi(s)
		if err != nil {
			send(ctx, 500, err.Error(), nil)
			return
		}
	}
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 1
	}
	result, err := Cron.FindEntries(page, size)
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	send(ctx, 200, "", result)
	return
}

// 校验 job 字段参数是否正确
func checkJobFields(v map[string]interface{}) error {
	if v["name"].(string) != "" && utf8.RuneCountInString(v["name"].(string)) > 20 {
		return fmt.Errorf("job name must be less or equal then 20 rune")
	}
	if v["exec"].(string) != "" && utf8.RuneCountInString(v["exec"].(string)) > 100 {
		return fmt.Errorf("job exec must be less or equal then 100 rune")
	}
	if v["desc"].(string) != "" && utf8.RuneCountInString(v["desc"].(string)) > 255 {
		return fmt.Errorf("job exec must be less or equal then 255 rune")
	}

	// 校验定时任务语法是否正确
	if v["spec"].(string) != "" {
		if _, err := Parse(v["spec"].(string)); err != nil {
			return fmt.Errorf("spec parse error: " + err.Error())
		}
	}
	return nil
}

// http 响应输出
func send(ctx *fasthttp.RequestCtx, status int, msg string, data interface{}) {
	rep := map[string]interface{}{}
	ctx.SetStatusCode(status)
	rep["status"] = status
	rep["msg"] = msg
	if data != nil {
		rep["data"] = data
	}
	res, err := json.Marshal(rep)
	if err != nil {
		rep["status"] = 500
		ctx.SetStatusCode(500)
		rep["msg"] = err.Error()
	}
	fmt.Fprintf(ctx, string(res))
}

// 结点内部数据同步接口
func syncData(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html")
	body := ctx.PostBody()
	err := JobQueue.SyncJob(ctx, body)
	if err != nil {
		send(ctx, 500, err.Error(), nil)
		return
	}
	send(ctx, 200, "ok", nil)
	return
}
