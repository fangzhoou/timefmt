package service

import (
    "fmt"
    "regexp"
    "strings"

    "github.com/valyala/fasthttp"
)

type Handler struct {
}

// 添加任务
func (h *Handler) addJob(ctx *fasthttp.RequestCtx) {
    a := ctx.Value("aa")

    fmt.Fprintf(ctx, a.(string), "add job", string(ctx.Path()))
}

// 修改任务
func (h *Handler) updateJob(ctx *fasthttp.RequestCtx) {
    fmt.Fprintf(ctx, "update job", string(ctx.Path()))
}

// 删除任务
func (h *Handler) delJob(ctx *fasthttp.RequestCtx) {
    fmt.Fprintf(ctx, "delete job")
}

// 查询单个任务
func (h *Handler) findSingleJob(ctx *fasthttp.RequestCtx) {
    fmt.Fprintf(ctx, "find single job")
}

// 查询多个任务
func (h *Handler) findJobs(ctx *fasthttp.RequestCtx) {
    fmt.Fprintf(ctx, "find jobs")
}

// 404 路由未找到
func (h *Handler) notFound(ctx *fasthttp.RequestCtx) {
    fmt.Fprintf(ctx, "404 not found")
}

// 处理 fastHttp 请求
func (h *Handler) HandleFastHttp(ctx *fasthttp.RequestCtx) {
    method := strings.ToLower(string(ctx.Method()))
    switch string(ctx.Path()) {
    case "/job": // 任务
        // 处理不同请求方式
        switch method {
        case "get": // get 获取任务列表
            h.findJobs(ctx)
        case "post", "put": //  post，put 新增任务
            h.addJob(ctx)
        case "path": // update 更新任务
            h.updateJob(ctx)
        case "delete": // delete 删除任务
            h.delJob(ctx)
        default:
            h.notFound(ctx) // 404
        }
    default:
        path := string(ctx.Path())
        r, _ := regexp.MatchString("^/job/\\d+", path)
        if method == "get" && r {
            // /job/{id} get 获取单个任务
            h.findSingleJob(ctx)
        } else {
            h.notFound(ctx)
        }
    }
}

// 监听并提供 http 服务
func ListenAndServe() {
    fmt.Println(111)
    h := &Handler{}
    fasthttp.ListenAndServe(":8080", h.HandleFastHttp)
}
