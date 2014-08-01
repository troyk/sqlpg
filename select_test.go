package sqlpg

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSelect(t *testing.T) {

	Convey("String", t, func() {
		s := Select("u.*").From("users u").Where("name = $1", "troy")

		So(s.String(), ShouldEqual, "SELECT u.*\nFROM users u\nWHERE (name = $1)")

		So(s.Args("mefirst"), ShouldResemble, []interface{}{"mefirst", "troy"})

	})
}

func TestSelectFrom(t *testing.T) {

	Convey("String", t, func() {
		s := SelectFrom("SELECT u.* FROM users u").Where("name = $1", "troy").Limit(5)

		So(s.String(), ShouldEqual, "SELECT u.* FROM users u\nWHERE (name = $1)\nLIMIT 5")

		So(s.Args("mefirst"), ShouldResemble, []interface{}{"mefirst", "troy"})

	})
}
