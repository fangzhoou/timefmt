package core

import (
    "encoding/json"
    "fmt"
    "github.com/valyala/fasthttp"
    "regexp"
    "strconv"
    "strings"
    "unicode/utf8"
)

type Handler struct {
}

// 添加任务
// @param name string 任务名称
// @param spec string 时间描述
// @param mode string 执行方式
// @param exec string 执行语句
// @param args map[string]interface{} 附带参数
// @param depend []uint64 依赖的任务 id
// @param exec_num int 执行机器数
// @param desc string 任务描述
func (h *Handler) addJob(ctx *fasthttp.RequestCtx) {
    j := &job{}
    err := json.Unmarshal(ctx.PostBody(), &j)
    if err != nil {
        h.send(ctx, 400, err.Error(), nil)
        return
    }
    if j.Name == "" {
        h.send(ctx, 400, "param name is required", nil)
        return
    }
    if j.Spec == "" {
        h.send(ctx, 400, "param spec is required", nil)
        return
    }
    if j.Mode == "" {
        h.send(ctx, 400, "param mode is required", nil)
        return
    }
    if j.Exec == "" {
        h.send(ctx, 400, "param exec is required", nil)
        return
    }
    err = checkJobFields(j)
    if err != nil {
        h.send(ctx, 400, err.Error(), nil)
        return
    }

    err = JobQueue.Add(ctx, j)
    if err != nil {
        h.send(ctx, 500, err.Error(), nil)
        return
    }
    h.send(ctx, 200, "ok", nil)
    return
}

// 修改任务
// @param name string 任务名称
// @param spec string 时间描述
// @param mode string 执行方式
// @param exec string 执行语句
// @param args map[string]interface{} 附带参数
// @param depend []uint64 依赖的任务 id
// @param exec_num int 执行机器数
// @param desc string 任务描述
// @param status string 任务状态
func (h *Handler) updateJob(ctx *fasthttp.RequestCtx, id int) {
    j := &job{}
    err := json.Unmarshal(ctx.PostBody(), &j)
    if err != nil {
        h.send(ctx, 400, err.Error(), nil)
        return
    }
    log.Debug(j)
    err = checkJobFields(j)
    if err != nil {
        h.send(ctx, 400, err.Error(), nil)
        return
    }
    job, err := JobQueue.UpdateById(id, j)
    if err != nil {
        h.send(ctx, 400, err.Error(), nil)
        return
    }
    h.send(ctx, 200, "ok", job)
    return
}

// 删除任务
func (h *Handler) delJob(ctx *fasthttp.RequestCtx, id int) {
    j := &job{Status: JobStatusDisable}
    job, err := JobQueue.UpdateById(id, j)
    if err != nil {
        h.send(ctx, 400, err.Error(), nil)
        return
    }
    h.send(ctx, 200, "ok", job)
    return
}

// 查询单个任务
func (h *Handler) findJob(ctx *fasthttp.RequestCtx, id int) {
    job, err := JobQueue.FindJobById(id)
    if err != nil {
        h.send(ctx, 500, err.Error(), nil)
        return
    }
    h.send(ctx, 200, "", job)
    return
}

// 查询多个任务
// @param page int
// @param size int 默认 10
func (h *Handler) findJobs(ctx *fasthttp.RequestCtx) {
    page := 1
    size := 10
    if len(ctx.PostBody()) > 0 {
        args := map[string]string{}
        err := json.Unmarshal(ctx.PostBody(), &args)
        if err != nil {
            h.send(ctx, 400, err.Error(), nil)
            return
        }
        if args["page"] != "" {
            p, err := strconv.Atoi(args["page"])
            if err != nil {
                h.send(ctx, 400, err.Error(), nil)
                return
            }
            if p > 0 {
                page = p
            }
        }
        if args["size"] != "" {
            s, err := strconv.Atoi(args["size"])
            if err != nil {
                h.send(ctx, 400, err.Error(), nil)
                return
            }
            if s > 0 {
                size = s
            }
        }
    }

    result, err := JobQueue.FindJobList(page, size)
    if err != nil {
        // 当前页没有数据
        h.send(ctx, 200, "no data", nil)
        return
    }
    h.send(ctx, 200, "", result)
    return
}

// 开启某个任务
func (h *Handler) setJobOn(ctx *fasthttp.RequestCtx, id int) {
    j := &job{Status: JobStatusOn}
    job, err := JobQueue.UpdateById(id, j)
    if err != nil {
        h.send(ctx, 400, err.Error(), nil)
        return
    }
    h.send(ctx, 200, "ok", job)
    return
}

// 暂停某个任务
func (h *Handler) setJobOff(ctx *fasthttp.RequestCtx, id int) {
    j := &job{Status: JobStatusOff}
    job, err := JobQueue.UpdateById(id, j)
    if err != nil {
        h.send(ctx, 400, err.Error(), nil)
        return
    }
    h.send(ctx, 200, "ok", job)
    return
}

// 404 路由未找到
func (h *Handler) notFound(ctx *fasthttp.RequestCtx) {
    h.send(ctx, 404, "page not found", nil)
    return
}

// http 响应输出
func (h *Handler) send(ctx *fasthttp.RequestCtx, status int, msg string, data interface{}) {
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

// 处理 http 请求
// 提供对外 http RESTFul 接口，地址：0.0.0.0:port
// /job [post/put] 添加任务
// /job [get] 获取任务列表
// /job/{id} [get] 获取单个任务
// /job/{id} [patch] 修改单个任务属性
// /job/{id} [delete] 删除单个任务
// /job/{id}/on [put/patch] 开启任务
// /job/{id}/off [put/patch] 关闭任务
func (h *Handler) HandleFastHttp(ctx *fasthttp.RequestCtx) {
    method := strings.ToLower(string(ctx.Method()))
    log.Debug(string(ctx.Path()), method, string(ctx.PostBody()))
    switch string(ctx.Path()) {
    case "/job":
        // 处理不同请求方式
        switch method {

        // get 获取任务列表
        case "get":
            h.findJobs(ctx)

        // post，put 新增任务
        case "post", "put":
            h.addJob(ctx)

        // 404
        default:
            h.notFound(ctx)
        }
    default:
        p := string(ctx.Path())
        b, _ := regexp.MatchString("^/job/\\d+$", p)
        if b {
            sp := strings.Split(p, "/")
            id, err := strconv.Atoi(sp[len(sp)-1])
            if err != nil {
                h.send(ctx, 400, err.Error(), nil)
            }

            switch method {

            // /job/{id} get 获取单个任务
            case "get":
                h.findJob(ctx, id)

            // /job/{id} patch 修改单个任务
            case "patch":
                h.updateJob(ctx, id)

            // /job/{id} delete 删除单个任务
            case "delete":
                h.delJob(ctx, id)

            // 404
            default:
                h.notFound(ctx)
            }
        } else {
            // /job/{id}/on [put/patch] 启用任务
            if b, _ := regexp.MatchString("^/job/\\d+/on$", p); b {
                sp := strings.Split(p, "/")
                id, err := strconv.Atoi(sp[len(sp)-2])
                if err != nil {
                    h.send(ctx, 400, err.Error(), nil)
                }
                h.setJobOn(ctx, id)
            } else if b, _ := regexp.MatchString("^/job/\\d+/off$", p); b {
                // /job/{id}/off [put/patch] 停止任务
                sp := strings.Split(p, "/")
                id, err := strconv.Atoi(sp[len(sp)-2])
                if err != nil {
                    h.send(ctx, 400, err.Error(), nil)
                }
                h.setJobOff(ctx, id)
            } else {
                h.notFound(ctx)
            }
            //h.notFound(ctx)
        }
    }
}

// 校验 job 字段参数是否正确
func checkJobFields(j *job) error {
    if utf8.RuneCountInString(j.Name) > 20 {
        return fmt.Errorf("job name must be less or equal then 20 rune")
    }
    if utf8.RuneCountInString(j.Exec) > 100 {
        return fmt.Errorf("job exec must be less or equal then 100 rune")
    }
    if utf8.RuneCountInString(j.Desc) > 255 {
        return fmt.Errorf("job exec must be less or equal then 255 rune")
    }
    if j.Status != 0 &&
        j.Status != JobStatusOn &&
        j.Status != JobStatusOff &&
        j.Status != JobStatusDisable {
        return fmt.Errorf("invalid job status value")
    }

    // 校验定时任务语法是否正确
    if j.Spec != "" {
        if _, err := Parse(j.Spec); err != nil {
            return fmt.Errorf("spec parse error: " + err.Error())
        }
    }
    return nil
}

// 监听并提供 http 服务
func ListenAndServe() {
    h := &Handler{}
    port := strconv.Itoa(Conf.Port)
    go fasthttp.ListenAndServe(":"+port, h.HandleFastHttp)
    log.Info("listening for client requests on localhost:" + port)
}
