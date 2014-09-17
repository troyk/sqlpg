package sqlpg

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSchema(t *testing.T) {

	type Ad struct {
		Id                     string   `json:"id"`
		OwnlocalId             int      `json:"ownlocal_id"`
		BusinessId             string   `json:"business_id"`
		PublisherId            string   `json:"publisher_id"`
		CustomId               string   `json:"custom_id"`
		BatchId                string   `json:"batch_id"`
		JobId                  string   `json:"job_id"`
		CreatedAt              Time     `json:"created_at"`
		UpdatedAt              Time     `json:"updated_at"`
		Type                   string   `json:"ad_type"`
		House                  bool     `json:"house"`
		GoogleMapAddress       string   `json:"gmap_address"`
		MultiPage              bool     `json:"multi_page"`
		Coupon                 bool     `json:"coupon"`
		JPGUrl                 string   `json:"jpg_url"`
		PDFUrl                 string   `json:"pdf_url"`
		PDFFingerprint         string   `json:"pdf_fingerprint"`
		Language               string   `json:"language"`
		Content                string   `json:"content"`
		UnkownWords            []string `json:"unknown_words"`
		RecommendedCategoryIds []int    `json:"recommended_category_ids"`
		RecommendedBusinessIds []int    `json:"recommended_business_ids"`
	}

	Convey("ToDoNameMe", t, func() {
		db := MustOpen(fmt.Sprintf("postgres://%s:%s@%s/adgasm?sslmode=disable", "troy", "", "localhost"))

		Convey("GetSchema", func() {
			schema, err := GetSchema(db)
			So(err, ShouldBeNil)
			So(schema, ShouldNotBeNil)
			//fmt.Printf("%+v", schema["ads"])

		})

		Convey("GetTableSchema", func() {
			schema, err := GetTableSchema(db, "ads")
			So(err, ShouldBeNil)
			So(schema.Columns, ShouldNotBeEmpty)
			So(schema.PrimaryKeys, ShouldNotBeEmpty)

			// get all columns by default
			matched := schema.ColumnsByName()
			So(len(matched), ShouldEqual, len(schema.Columns))
			// match specified in column order
			matched = schema.ColumnsByName("id, pdf_url", "batch_id", "foo")
			So(len(matched), ShouldEqual, 3)
			So(matched[0].Name, ShouldEqual, "id")
			So(matched[2].Name, ShouldEqual, "pdf_url")

		})

		Convey("InsertMap", func() {
			//ad := Ad{OwnlocalId: 0, UnkownWords: []string{"troy", "rob"}}
			ad := map[string]interface{}{"ownlocal_id": 1}
			builder, err := newUpsertBuilder(db, "ads")
			So(err, ShouldBeNil)
			sql, values := builder.InsertSql(ad)
			So(sql, ShouldEqual, "INSERT INTO ads(ownlocal_id) VALUES(nullif($1,0)::integer)")
			So(values[0], ShouldEqual, 1)
		})

		Convey("UpdateMap", func() {
			//ad := Ad{OwnlocalId: 0, UnkownWords: []string{"troy", "rob"}}
			ad := map[string]interface{}{"ownlocal_id": 1}
			builder, err := newUpsertBuilder(db, "ads")
			So(err, ShouldBeNil)
			sql, values := builder.UpdateSql(ad, "theuuid")
			So(sql, ShouldEqual, "UPDATE ads SET ownlocal_id = nullif($1,0)::integer WHERE id=$2")
			So(values[0], ShouldEqual, 1)
		})

	})

	Convey("Column", t, func() {
		Convey("ToHolder", func() {
			Convey("Positional named and index not null", func() {
				col := columnSchema{Name: "id", Type: "uuid", NotNull: true}
				So(col.ToValueHolder(0), ShouldEqual, ":id")
				So(col.ToValueHolder(1), ShouldEqual, "$1")
			})
			Convey("Positional named and index can be null", func() {
				col := columnSchema{Name: "id", Type: "uuid"}
				So(col.ToValueHolder(0), ShouldEqual, "nullif(:id,'')::uuid")
				So(col.ToValueHolder(1), ShouldEqual, "nullif($1,'')::uuid")
			})

			Convey("coalesce(nullif) when not null and default", func() {
				col := columnSchema{Name: "id", Type: "uuid", NotNull: true, Default: "gen_random_uuid()"}
				So(col.ToValueHolder(1), ShouldEqual, "coalesce(nullif($1,'')::uuid,gen_random_uuid())")
				col = columnSchema{Name: "created_at", Type: "timestamptz", NotNull: true, Default: "now()"}
				So(col.ToValueHolder(1), ShouldEqual, "coalesce(nullif($1,'0001-01-01 00:00:00 zulu')::timestamptz,now())")
				col = columnSchema{Name: "updated_at", Type: "timestamptz"}
				So(col.ToValueHolder(1), ShouldEqual, "nullif($1,'0001-01-01 00:00:00 zulu')::timestamptz")
				col = columnSchema{Name: "is_true", Type: "boolean", NotNull: true, Default: "false"}
				So(col.ToValueHolder(1), ShouldEqual, "coalesce(nullif($1,false)::boolean,false)")
				col = columnSchema{Name: "language", Type: "iso639lang", NotNull: true, Default: "'en'::iso639lang"}
				So(col.ToValueHolder(1), ShouldEqual, "coalesce(nullif($1,'')::iso639lang,'en'::iso639lang)")

			})

		})
	})

}
