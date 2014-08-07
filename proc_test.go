package sqlpg

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestProc(t *testing.T) {

	Convey("Replaces empty name holders", t, func() {
		p := Proc("users.save(:id, _email:=:email, _password := :password , _name := :name)")

		So(p.String(), ShouldEqual, "users.save()")

		p2 := p.Set("id", 1)

		So(p2.String(), ShouldEqual, "users.save($1)")

		p2 = p2.Set("email", "t@me.com")
		So(p2.String(), ShouldEqual, "users.save($1, _email:=$2)")

	})

	Convey("Set params only filter", t, func() {
		p := Proc("users.save(:id, _email:=:email, _password := :password , _name := :name)")

		params := map[string]interface{}{"password": "foo", "name": "tyrone"}

		p2 := p.SetParams(params).Set("id", 69)
		So(p2.String(), ShouldEqual, "users.save($1,  _password := $2 , _name := $3)")

		p3 := p.SetParams(params, "name").Set("id", 69)
		So(p3.String(), ShouldEqual, "users.save($1,   _name := $2)")

		//p2 := p.SetParams(params, []string{}).Set("id", 69)

	})
}
