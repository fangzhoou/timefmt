package utils

import (
    "crypto/md5"
    "encoding/hex"
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

// 获取 md5 值
func Md5(str string) string {
    m := md5.New()
    m.Write([]byte(str))
    md5Data := m.Sum([]byte(""))
    return hex.EncodeToString(md5Data)
}
