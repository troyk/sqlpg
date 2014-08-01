package sqlpg

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDBProxy(t *testing.T) {

	Convey("Transactions", t, func() {
		db, err := Open("postgres://troy@localhost/adgasm?sslmode=disable&application_name=server")
		So(err, ShouldBeNil)

		const uid = "69eeeeea-046d-4866-bc41-01b510bef61a"

		var nameNow string
		id1, _ := db.GetInt("select txid_current();")
		nameBefore, _ := db.GetString("select name from users where id=$1", uid)
		So(err, ShouldBeNil)
		So(nameBefore, ShouldEqual, "Troy")

		tx, err := db.Begin()
		So(err, ShouldBeNil)

		tx.Exec("update users set name='foo' where id=$1", uid)
		// should have new txid
		id2, _ := tx.GetInt("select txid_current();")
		So(id1, ShouldNotEqual, id2)

		// reading from the db should grab from outside the tx
		nameNow, err = db.GetString("select name from users where id=$1", uid)
		So(err, ShouldBeNil)
		So(nameNow, ShouldEqual, nameBefore)

		// reading from tx should get the update from above
		nameNow, err = tx.GetString("select name from users where id=$1", uid)
		So(err, ShouldBeNil)
		So(nameNow, ShouldEqual, "foo")

		// open new inner tx
		tx2, err2 := tx.Begin()
		So(err2, ShouldBeNil)
		//id3, _ := tx2.GetInt("select txid_current();")
		//So(id3, ShouldNotEqual, id2) // savepoints share txid

		// inner tx nme should still be foo
		nameNow, err = tx2.GetString("select name from users where id=$1", uid)
		So(err, ShouldBeNil)
		So(nameNow, ShouldEqual, "foo")
		tx2.Exec("update users set name='fooInner' where id=$1", uid)
		nameNow, err = tx2.GetString("select name from users where id=$1", uid)
		So(err, ShouldBeNil)
		So(nameNow, ShouldEqual, "fooInner")
		tx2.Rollback()

		nameNow, err = tx.GetString("select name from users where id=$1", uid)
		So(err, ShouldBeNil)
		So(nameNow, ShouldEqual, "foo")

		//		var data map[string]interface{}

		err = tx.Rollback()
		nameNow, err = db.GetString("select name from users where id=$1", uid)
		So(err, ShouldBeNil)
		So(nameNow, ShouldEqual, nameBefore)

	})
}
