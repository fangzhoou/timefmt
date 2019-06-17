package timefmt

import (
	"testing"
)

func Test_Datetime(t *testing.T) {
	t.Log(Datetime())
}

func Test_Format(t *testing.T) {
	t.Log(Format("Y-M-D H:I:S"))
	t.Log(Format("YY-MM-DD A HH:II:SS"))
	t.Log(Format("YYYY-MM-DD A HH:II:SS", 1559211727))
	t.Log(Format("y-m-d h:i:s", 1559211727))
	t.Log(Format("yy-mm-dd hh:ii:ss", 1559211727))
	t.Log(Format("yyyy-mm-dd a hh:ii:ss", 1559211727))
	t.Log(Format("Y年M月D日 H时I分s秒"))
}

func Test_Micro(t *testing.T) {
	t.Log(Millisecond())
	t.Log(Nanosecond())
	t.Log(Time())
}