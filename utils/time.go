package utils

import "time"

const (
    Layout = "2006-01-02 15:04:05"
)

// 字符串转时间，支持 datetime 格式：2006-01-02 15:04:05
func StrToTime(str string) time.Time {
    loc, _ := time.LoadLocation("Local")
    t, _ := time.ParseInLocation(Layout, str, loc)
    return t
}
