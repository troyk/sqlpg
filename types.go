package sqlpg

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Time type to work with pg's milliseconds json types
// postgres changed their json date formatting as of 9.4beta2 to not suck
// >= 9.4b2 "2014-09-03T23:31:27.344959+00:00"
// <  9.4b2 "2014-09-16 21:25:25.43576-07"

const pgJsonTimeLayout = "2006-01-02 15:04:05.999999999-07"

type Time struct {
	time.Time
}

func (pgt *Time) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	var (
		t   time.Time
		err error
	)
	if s == "" {
		t = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	} else if strings.Index(s, "T") > 0 {
		t, err = time.Parse(time.RFC3339Nano, s)
	} else {
		t, err = time.Parse(pgJsonTimeLayout, s)
	}
	if err != nil {
		return err
	}
	pgt.Time = t
	return nil
}

func (pgt *Time) Scan(src interface{}) error {
	if src == nil {
		pgt.Time = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
		return nil
	}
	pgt.Time = src.(time.Time)
	return nil
}

// Value implements the driver Valuer interface.
func (pgt Time) Value() (driver.Value, error) {
	if pgt.IsZero() {
		return nil, nil
	}
	return pgt.Time, nil
}

// StringSlice is the go equiv of text[]
type StringSlice []string

func (s StringSlice) Value() (driver.Value, error) {
	if s == nil {
		return nil, nil
	}
	var (
		buffer bytes.Buffer
		last   = len(s) - 1
	)
	buffer.WriteString("{")
	for i, str := range s {
		if i == last {
			buffer.WriteString("'" + strings.Replace(str, `'`, `\'`, -1) + "'")
		} else {
			buffer.WriteString("'" + strings.Replace(str, `'`, `\'`, -1) + "',")
		}

	}
	buffer.WriteString("}")
	return buffer.String(), nil
}

// StringSlice is the go equiv of int[]
type IntSlice []int

func (s IntSlice) Value() (driver.Value, error) {
	if s == nil {
		return nil, nil
	}
	var (
		buffer bytes.Buffer
		last   = len(s) - 1
	)
	buffer.WriteString("{")
	for i, val := range s {
		if i == last {
			buffer.WriteString(strconv.Itoa(val))
		} else {
			buffer.WriteString(strconv.Itoa(val) + ",")
		}

	}
	buffer.WriteString("}")
	return buffer.String(), nil
}

// uuid type
type UUID string

func (uuid UUID) String() string {
	return string(uuid)
}
