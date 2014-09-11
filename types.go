package sqlpg

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

// Time type to work with pg's milliseconds json types
const pgJsonTimeLayout = "2006-01-02 15:04:05.999999999-07"

type Time struct {
	time.Time
}

func (pgt *Time) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		pgt.Time = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	} else {
		t, err := time.Parse(pgJsonTimeLayout, s)
		if err != nil {
			return err
		}
		pgt.Time = t
	}
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

// uuid type
type UUID string

func (uuid UUID) String() string {
	return string(uuid)
}
