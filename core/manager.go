package core

import (
    "fmt"

    "github.com/fangzhoou/dcron/utils"
)

// 添加服务节点
func addCronNode() {
    ip, err := utils.GetLocalIP()
    fmt.Println(ip, err)
}
