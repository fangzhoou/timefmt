package core

import (
    "encoding/json"
    "fmt"
    "regexp"
    "strconv"
    "strings"

    "github.com/valyala/fasthttp"
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
    err = JobQueue.Add(ctx, j)
    if err != nil {
        h.send(ctx, 500, err.Error(), nil)
        return
    }
    h.send(ctx, 200, "success", nil)
    return
}

// 修改任务
// @param name string 任务名称
// @param spec string 时间描述
// @param mode string 执行方式
// @param exec string 执行语句
// @param args map[string]interface{} 附带参数
// @param depend []uint64 依赖的任务 id
// @param desc string 任务描述
func (h *Handler) updateJob(ctx *fasthttp.RequestCtx, id int) {
    //h.send([]byte("update job"))
}

// 删除任务
func (h *Handler) delJob(ctx *fasthttp.RequestCtx, id int) {
    //h.send([]byte("delete job"))
}

// 查询单个任务
func (h *Handler) findJob(ctx *fasthttp.RequestCtx, id int) {
    job, err := JobQueue.GetJobById(id)
    if err != nil {
        h.send(ctx, 500, err.Error(), nil)
        return
    }
    h.send(ctx, 200, "", job)
    return
}

// 查询多个任务
func (h *Handler) findJobs(ctx *fasthttp.RequestCtx) {
    //h.send([]byte("find job"))
}

// 404 路由未找到
func (h *Handler) notFound(ctx *fasthttp.RequestCtx) {
    //h.send([]byte("404 not found"))
}

// http 响应输出
func (h *Handler) send(ctx *fasthttp.RequestCtx, status int, msg string, data interface{}) {
    rep := map[string]interface{}{}
    rep["status"] = status
    rep["msg"] = msg
    if data != nil {
        rep["data"] = data
    }
    res, _ := json.Marshal(rep)
    ctx.SetStatusCode(status)
    fmt.Fprintf(ctx, string(res))
}

// 处理 http 请求
// 提供对外 http RESTFul 接口，地址：0.0.0.0:port
// /job [post/put] 添加任务
// /job [get] 获取任务列表
// /job/{id} [get] 获取单个任务
// /job/{id} [patch] 修改任务属性
// /job/{id} [delete] 删除单个任务
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
            h.notFound(ctx)
        }
    }
}

// 监听并提供 http 服务
func ListenAndServe() {
    h := &Handler{}
    port := strconv.Itoa(Conf.Port)
    fasthttp.ListenAndServe(":"+port, h.HandleFastHttp)
    log.Info("listening for client requests on localhost:", port)
}
