package timefmt

import (
	"strconv"
	"strings"
	"time"
)

const (
	layout = "2006-01-02 15:04:05" // go 自恋时间
)

// 支持的时间格式
var timeFormat = map[string]int{
	"Y":    1,
	"YY":   1,
	"YYYY": 1,
	"y":    1,
	"yy":   1,
	"yyyy": 1,
	"M":    1,
	"MM":   1,
	"m":    1,
	"mm":   1,
	"D":    1,
	"DD":   1,
	"d":    1,
	"dd":   1,
	"H":    1,
	"HH":   1,
	"h":    1,
	"hh":   1,
	"I":    1,
	"II":   1,
	"i":    1,
	"ii":   1,
	"S":    1,
	"SS":   1,
	"s":    1,
	"ss":   1,
	"A":    1,
	"a":    1,
}

// 获取当前时间戳，秒
func Time() int64 {
	return time.Now().Unix()
}

// 获取当前时间戳，毫秒
func Millisecond() int64 {
	n := time.Now().UnixNano()
	return n / 1e6
}

// 获取当前时间戳，纳秒
func Nanosecond() int64 {
	return time.Now().UnixNano()
}

// 获取当前日期时间，datetime 格式：YYYY-mm-dd H:i:s
// return string，如：2019-05-22 22:36:20
func Datetime() string {
	t := time.Now()
	return t.Format(layout)
}

// 按 pattern 模式返回当前时间或传入时间戳 s 的格式化时间
// 支持的格式：
// Y: 年份 4 位，如：2019
// YY：年份 2 位，如：19
// YYYY：年份 4 位，如：2019
// y: 年份 2 位，如：19
// yy: 年份 2 位，如：19
// yyyy：年份 4 位，如：2019
// M：月份，有前导 0，取值 0~12 如：02
// MM：月份，有前导 0，取值 0~12 如：02
// m：月份，无前导 0，取值 0~12 如：2
// mm：月份，有前导 0，取值 0~31 如：02
// D：日，有前导 0，取值 0~31 如：05
// DD：日，有前导 0，取值 0~31 如：05
// d：日，无前导 0，如：5
// dd：日，有前导 0，如：05
// H: 小时，24 小时制，有前导 0，取值 0~24 如：23
// HH：小时，24 小时制，有前导 0，取值 0~24 如：23
// h: 小时，12 小时制，无前导 0，取值 0~12 如：8
// hh：小时，12 小时制，有前导 0，取值 0~12 如：08
// I: 分钟，有前导 0，取值 0~60 如：01
// II: 分钟，有前导 0，取值 0~60 如：01
// i: 分钟，无前导 0，取值 0~61 如：1
// ii: 分钟，有前导 0，取值 0~61 如：01
// S: 秒，有前导 0，取值 0~60 如：03
// SS: 秒，有前导 0，取值 0~60 如：03
// s: 秒，无前导 0，取值 0~60 如：3
// ss: 秒，有前导 0，取值 0~60 如：03
// a：上午或下午，小写：am 或 pm
// A：上午或下午，大写：AM 或 PM
func Format(pattern string, s ...int64) string {
	var t time.Time
	if len(s) > 0 {
		t = time.Unix(s[0], 0)
	} else {
		t = time.Now()
	}

	// 遍历字符串，读取时间相关字符
	p := []rune(pattern)
	var sl []string
	var prev rune
	for _, v := range p {
		strV := string(v)
		if _, ok := timeFormat[strV]; ok {
			if v == prev {
				sl[len(sl)-1] += strV
			} else {
				sl = append(sl, strV)
			}
			prev = v
		} else {
			sl = append(sl, strV)
		}
	}


	// 获取时间字符对应的时间
	for k, v := range sl {
		if _, ok := timeFormat[v]; ok {
			sl[k] = getTimeStr(v, t)
		}
	}
	return strings.Join(sl, "")
}

// 读取时间
func getTimeStr(s string, t time.Time) string {
	switch s {
	case "Y":
		fallthrough
	case "YYYY":
		fallthrough
	case "yyyy":
		return strconv.Itoa(t.Year())
	case "y":
		fallthrough
	case "yy":
		fallthrough
	case "YY":
		year := strconv.Itoa(t.Year())
		return string(year[2]) + string(year[3])

	case "M":
		fallthrough
	case "MM":
		fallthrough
	case "mm":
		month := t.Month()
		if month < 10 {
			return "0" + strconv.Itoa(int(month))
		} else {
			return strconv.Itoa(int(month))
		}
	case "m":
		return strconv.Itoa(int(t.Month()))

	case "D":
		fallthrough
	case "DD":
		fallthrough
	case "dd":
		day := t.Day()
		if day < 10 {
			return "0" + strconv.Itoa(day)
		} else {
			return strconv.Itoa(day)
		}
	case "d":
		return strconv.Itoa(t.Day())

	case "H":
		fallthrough
	case "HH":
		hour := t.Hour()
		if hour < 10 {
			return "0" + strconv.Itoa(hour)
		} else {
			return strconv.Itoa(hour)
		}
	case "h":
		fallthrough
	case "hh":
		hour := t.Hour()
		if hour > 12 {
			hour -= 12
		}
		return strconv.Itoa(hour)

	case "I":
		fallthrough
	case "II":
		fallthrough
	case "ii":
		min := t.Minute()
		if min < 10 {
			return "0" + strconv.Itoa(min)
		} else {
			return strconv.Itoa(min)
		}
	case "i":
		return strconv.Itoa(t.Minute())

	case "S":
		fallthrough
	case "SS":
		fallthrough
	case "ss":
		sec := t.Second()
		if sec < 10 {
			return "0" + strconv.Itoa(sec)
		} else {
			return strconv.Itoa(sec)
		}
	case "s":
		return strconv.Itoa(t.Second())

	case "a":
		hour := t.Hour()
		if hour <= 12 {
			return "am"
		} else {
			return "pm"
		}
	case "A":
		hour := t.Hour()
		if hour <= 12 {
			return "AM"
		} else {
			return "PM"
		}
	}
	return ""
}