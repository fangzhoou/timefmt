package utils

import (
    "errors"
    "net"
)

// 获取本地 ip
func GetLocalIP() (ip string, err error) {
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        return
    }
    for _, addr := range addrs {
        if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
            if ipnet.IP.To4() != nil {
                ip = ipnet.IP.String()
                return
            }
        }
    }
    err = errors.New("IP not found")
    return
}
