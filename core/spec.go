package core

import (
	"time"
)

// 定时任务语法分析结果存储对象
type SpecSchedule struct {
	Second, Minute, Hour, Dom, Month, Dow uint64
}

// 获取任务下次执行时间
func (s *SpecSchedule) Next(t time.Time) time.Time {
	// 重置起始时间，下一秒开始
	// 如果不从下一秒开始，当前秒可能会多次执行同一任务
	t = t.Add(1*time.Second - time.Duration(t.Nanosecond())*time.Nanosecond)

	added := false

	// 定时任务最长等待时间 5 年，超过 5 年默认不执行
	yearLimit := t.Year() + 5

	// 6位时间位：秒 分 时 日 月 周
	// 每个时间点循环到起始位时，其前一位时间可能被加 1 了，导致前一个时间位不匹配了。
	// 依次类推，循环到起始位的时间点，其前面所有时间点都可能被加 1 了，所以需要重新开始每个时间位匹配。
WARP:

	if t.Year() > yearLimit {
		return time.Time{}
	}

	// 先比较月份，匹配到第一个执行月跳出，否则月份加 1
	for 1<<uint64(t.Month())&s.Month == 0 {
		if !added {
			added = true
			// 第一次比较月份时，时间除年、月外其它时间位置为当前位起始值
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
		}
		t = t.AddDate(0, 1, 0) // 当前月份加 1

		// 一月重新循环
		if t.Month() == time.January {
			goto WARP
		}
	}

	// 再比较天，需要同时判断月份的天和周的天，是否都匹配
	for !(1<<uint(t.Day())&s.Dom > 0 && 1<<uint(t.Weekday())&s.Dow > 0) {
		if !added {
			added = true
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		}
		t = t.AddDate(0, 0, 1) // 当前天数加 1

		if t.Day() == 1 {
			goto WARP
		}
	}

	// 小时
	for 1<<uint64(t.Hour())&s.Hour == 0 {
		if !added {
			added = true
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
		}
		t = t.Add(1 * time.Hour)

		if t.Hour() == 0 {
			goto WARP
		}
	}

	// 分钟
	for 1<<uint64(t.Minute())&s.Minute == 0 {
		if !added {
			added = true
			t = t.Truncate(time.Minute)
		}
		t = t.Add(1 * time.Minute)

		if t.Minute() == 0 {
			goto WARP
		}
	}

	// 秒
	for 1<<uint64(t.Second())&s.Second == 0 {
		if !added {
			added = true
			t = t.Truncate(time.Second)
		}
		t = t.Add(1 * time.Second)

		if t.Second() == 0 {
			goto WARP
		}
	}
	return t
}
