package utils

import "testing"

func TestGetLocalIP(t *testing.T) {
    t.Log(GetLocalIP())
}

func TestMd5(t *testing.T) {
    t.Log(Md5("aa"))
}

func TestGetRootPath(t *testing.T) {
    t.Log(GetRootPath())
}
