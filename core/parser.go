package core

import (
    "fmt"
    "math"
    "strconv"
    "strings"
)

// 时间边界
type bounds struct {
    min, max uint64
    names    map[string]uint64
}

// 各时间项取值范围
var (
    seconds = bounds{0, 59, map[string]uint64{}}
    minutes = bounds{0, 59, map[string]uint64{}}
    hours   = bounds{0, 23, map[string]uint64{}}
    dom     = bounds{1, 31, map[string]uint64{}}
    months  = bounds{1, 12, map[string]uint64{
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }}
    dow = bounds{0, 6, map[string]uint64{
        "sun": 0,
        "mon": 1,
        "tue": 2,
        "wed": 3,
        "thu": 4,
        "fri": 5,
        "sat": 6,
    }}
)

// 解析定时任务语法格式，如；* * * * * *
// @param spec string 定时任务语法格式: 秒 分 时 日 月 周
//
// 字段介绍      | 是否必须？   | 允许的值          | 支持的字符
// ------------ | ---------- | --------------  | -------------
// Seconds      | Yes        | 0-59            | * / , -
// Minutes      | Yes        | 0-59            | * / , -
// Hours        | Yes        | 0-23            | * / , -
// Day of month | Yes        | 1-31            | * / , - ?
// Month        | Yes        | 1-12 or JAN-DEC | * / , -
// Day of week  | Yes        | 0-6 or SUN-SAT  | * / , - ?
func Parse(spec string) (Schedule, error) {
    if len(spec) == 0 {
        return nil, fmt.Errorf("empty spec string")
    }

    // 支持 @ 符，如：@hourly、@yearly、... TODO
    //if spec[0] == '@' {
    //    return nil, nil
    //}

    fields := strings.Fields(spec)
    var (
        bits uint64
        err  error
    )
    field := func(fd string, b bounds) uint64 {
        bits, err = parseField(fd, b)
        return bits
    }
    var (
        second = field(fields[0], seconds)
        minute = field(fields[1], minutes)
        hour   = field(fields[2], hours)
        dom    = field(fields[3], dom)
        month  = field(fields[4], months)
        dow    = field(fields[5], dow)
    )
    if err != nil {
        return nil, err
    }
    return &SpecSchedule{
        Second: second,
        Minute: minute,
        Hour:   hour,
        Dom:    dom,
        Month:  month,
        Dow:    dow,
    }, nil
}

// 解析单个 spec 语句
func parseField(field string, b bounds) (uint64, error) {
    var bits uint64
    ranges := strings.FieldsFunc(field, func(r rune) bool {
        return r == ','
    })
    for _, expr := range ranges {
        bit, err := getBit(expr, b)
        if err != nil {
            return bits, err
        }
        bits |= bit
    }
    return bits, nil
}

// 获取需执行的时间节点二进制标记位，需要执行的位置 1
func getBit(expr string, b bounds) (uint64, error) {
    var (
        start, end, step, bit uint64
        rangeAndStep          = strings.Split(expr, "/")
        lowAndHigh            = strings.Split(rangeAndStep[0], "-")
        singleDigit           = len(lowAndHigh) == 1
        err                   error
    )
    if lowAndHigh[0] == "*" || lowAndHigh[0] == "?" {
        start = b.min
        end = b.max
    } else {
        start, err = parseIntOrName(lowAndHigh[0], b.names)
        if err != nil {
            return 0, err
        }
        switch len(lowAndHigh) {
        case 1:
            end = start
        case 2:
            end, err = parseIntOrName(lowAndHigh[1], b.names)
            if err != nil {
                return 0, err
            }
        default:
            return 0, fmt.Errorf("too many hyphens: %s", expr)
        }
    }

    switch len(rangeAndStep) {
    case 1: // 没有 '/'
        step = 1
    case 2: // 有 '/'
        step, err = mustParseInt(rangeAndStep[1])
        if err != nil {
            return 0, err
        }
        if singleDigit {
            end = b.max
        }
    default:
        return bit, fmt.Errorf("too many slashes: %s", expr)
    }

    if start < b.min {
        return 0, fmt.Errorf("beginning of range (%d) below minimum (%d): %s", start, b.min, expr)
    }
    if end > b.max {
        return 0, fmt.Errorf("end of range (%d) above maximum (%d): %s", end, b.max, expr)
    }
    if start > end {
        return 0, fmt.Errorf("beginning of range (%d) beyond end of range (%d): %s", start, end, expr)
    }
    if step == 0 {
        return 0, fmt.Errorf("step of range should be a positive number: %s", expr)
    }

    if step == 1 {
        // 步长为 1 时特殊处理，二进制位 0 位为起始位，总长 64 位，
        // 如：start = 3，end = 5 时：^(1...1000000) & 111000 = 0...0111111 & 111000 = 111000，
        // 即 3-5 位为要执行的时间位
        bit = ^(math.MaxUint64 << (end + 1)) & (math.MaxUint64 << (start))
    } else {
        // 步长不为 1 时每次递增步长
        for i := start; i <= end; i += step {
            bit |= 1 << i
        }
    }
    return bit, nil
}

// 解析整数
func parseIntOrName(expr string, names map[string]uint64) (uint64, error) {
    if names != nil {
        if namedInt, ok := names[strings.ToLower(expr)]; ok {
            return namedInt, nil
        }
    }
    return mustParseInt(expr)
}

// 语句字符串解析为整数
func mustParseInt(expr string) (uint64, error) {
    num, err := strconv.Atoi(expr)
    if err != nil {
        return 0, fmt.Errorf("failed to parse int from %s: %s", expr, err)
    }
    if num < 0 {
        return 0, fmt.Errorf("negative number (%d) not allowed: %s", num, expr)
    }
    return uint64(num), nil
}
