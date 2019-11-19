package core

import (
    "crypto/md5"
    "encoding/hex"
    "net"
    "os"
    "path/filepath"
)

// 获取本地 ip
func GetLocalIP() (ip string) {
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
    ip = "127.0.0.1"
    return
}

// 获取 md5 值
func Md5(str string) string {
    m := md5.New()
    m.Write([]byte(str))
    md5Data := m.Sum([]byte(""))
    return hex.EncodeToString(md5Data)
}

// 获取当前根目录
func GetRootPath() (string, error) {
    dir, err := os.Getwd()
    if err != nil {
        return "", err
    }
    return filepath.Dir(dir), nil
}
