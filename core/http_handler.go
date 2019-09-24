// 提供对外 http RESTFul 接口，地址：0.0.0.0:port
// /job [post/put] 添加任务
// /job [path] 修改任务
// /job [get] 获取任务列表
// /job/{id} [get] 获取单个任务
// /job/{id} [delete] 删除单个任务

package core

import (
    "encoding/json"
    "fmt"
    "path"
    "regexp"
    "strconv"
    "strings"

    "github.com/valyala/fasthttp"
)

type Handler struct {
    ctx *fasthttp.RequestCtx
}

// 添加任务
// @param name string 任务名称
// @param spec string 时间描述
// @param type string 执行方式
// @param exec string 执行语句
// @param args map[string]interface{} 附带参数
// @param depend []uint64 依赖的任务 id
// @param desc string 任务描述
func (h *Handler) addJob() {
    p := &Job{Args: map[string]interface{}{}}
    err := json.Unmarshal(h.ctx.PostBody(), &p)
    if err != nil {
        fmt.Errorf(err.Error())
        h.send(400, "job args parse error", nil)
        return
    }
    if p.Name == "" || p.Spec == "" || p.Type == "" || p.Exec == "" {
        h.send(400, "args name、sepc、type、exec can't be empty", nil)
        return
    }

    // 添加任务
    err = JobQueue.Add(p)
    if err != nil {
        fmt.Errorf(err.Error())
        h.send(500, err.Error(), nil)
        return
    }
    h.send(200, "success", nil)
    return
}

// 修改任务
func (h *Handler) updateJob(id int) {
    //h.send([]byte("update job"))
}

// 删除任务
func (h *Handler) delJob(id int) {
    //h.send([]byte("delete job"))
}

// 查询单个任务
func (h *Handler) findJob(id int) {
    job, err := JobQueue.GetJobById(id)
    if err != nil {
        fmt.Errorf(err.Error())
        h.send(500, err.Error(), nil)
        return
    }
    h.send(200, "", job)
    return
}

// 查询多个任务
func (h *Handler) findJobs() {
    //h.send([]byte("find job"))
}

// 404 路由未找到
func (h *Handler) notFound() {
    //h.send([]byte("404 not found"))
}

// http 响应输出
func (h *Handler) send(status int, msg string, data interface{}) {
    rep := map[string]interface{}{}
    rep["status"] = status
    rep["msg"] = msg
    if data != nil {
        rep["data"] = data
    }
    res, _ := json.Marshal(rep)
    h.ctx.SetStatusCode(status)
    fmt.Fprintf(h.ctx, string(res))
}

// 处理 fastHttp 请求
func (h *Handler) HandleFastHttp(ctx *fasthttp.RequestCtx) {
    h.ctx = ctx
    method := strings.ToLower(string(ctx.Method()))
    fmt.Println(string(ctx.Path()), method, string(h.ctx.PostBody()))
    switch string(ctx.Path()) {
    case "/job": // 任务
        // 处理不同请求方式
        switch method {
        case "get": // get 获取任务列表
            h.findJobs()
        case "post", "put": // post，put 新增任务
            h.addJob()
        default:
            h.notFound() // 404
        }
    default:
        p := string(ctx.Path())
        b, _ := regexp.MatchString("^/job/\\d+", p)
        if b {
            _, f := path.Split(p)
            id, _ := strconv.Atoi(f)
            switch method {
            case "get": // /job/{id} get 获取单个任务
                h.findJob(id)
            case "patch": // /job/{id} patch 修改单个任务
                h.updateJob(id)
            case "delete": // /job/{id} delete 删除单个任务
                h.delJob(id)
            default:
                h.notFound()
            }
        } else {
            h.notFound()
        }
    }
}

// 监听并提供 http 服务
func ListenAndServe() {
    h := &Handler{}
    port := strconv.Itoa(Conf.Port)
    fasthttp.ListenAndServe(":"+port, h.HandleFastHttp)
}
