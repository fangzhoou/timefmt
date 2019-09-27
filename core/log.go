package core

import (
    "fmt"
    "os"
    "runtime/debug"
    "time"
)

type logger struct {
}

var log logger

const (
    // 日志级别
    Fatal = iota
    Error
    Warn
    Info
    Debug
)

// 日志级别名称
var LevelsName = [5]string{
    Fatal: "FATAL",
    Error: "ERROR",
    Warn:  "WARN",
    Info:  "INFO",
    Debug: "DEBUG",
}

// 致命错误处理
func (l *logger) Fatal(a ...interface{}) {
    l.Output(Fatal, a)
}

// 普通错误处理
func (l *logger) Error(a ...interface{}) {
    l.Output(Error, a)
}

// 警告处理
func (l *logger) Warn(a ...interface{}) {
    l.Output(Warn, a)
}

// 普通信息处理
func (l *logger) Info(a ...interface{}) {
    l.Output(Info, a)
}

// 调试信息处理
func (l *logger) Debug(a ...interface{}) {
    l.Output(Debug, a)
}

// 捕获错误，打印错误栈信息
func (l *logger) RecoverPanic(r interface{}) {
    if r != nil {
        l.Output(Fatal, []interface{}{r})
        debug.PrintStack()
        os.Exit(-2)
    }
}

// 输出错误
func (l *logger) Output(level int, a []interface{}) {
    fmt.Printf("%s %s |", time.Now().Format("2006-01-02 15:04:05.999999"), LevelsName[level])
    for _, v := range a {
        fmt.Printf(" %v", v)
    }
    fmt.Printf("\n")
}
