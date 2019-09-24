package snowflake

/*/
	Snowflake dialect provides the VARIANT data type. This is a snowflake column type that can
	resolve structs, maps and arrays. The dialect also supports defining columns of type
	snowflake.Object or snowflake.Array specifically (but under the hood, it's all handled as
	VARIANTs and we let snowflake manage the rest).

	In order to get things to work with arbitrary semi-structured data, all values are basically
	reduced to JSON values in a []byte. A set of helper functions allow the mashalling of the data
	in and out of the VARIANT type into specific data stuctures.

	Example:
		// create model with VARIANT column
		type Widget struct {
			ID  string            	`gorm:"column:ID;size:36"`
			Item snowflake.Variant 	`gorm:"column:ITEM;type:VARIANT"`
			CreatedAt time.Time 	`gorrm:'column:CREATED_AT:TIMESTAMP_TZ`
			UpdatedAt time.Time 	`gorrm:'column:CREATED_AT:TIMESTAMP_TZ`
		}

		// create data map for Item
		// (structs/arrays work too)
		itm := map[string]string{
			"Name":  "Bob A. Fett",
			"Address": "123 Somewhere",
			"Email": "Nah@notthere.net",
			"Company": "Empire Bounty, Inc",
		}

		// wrap in Variant
		var vitm snowlake.Variant
		if err := snowflake.NewVariant(&vitm); err != nil {
			// json error
			panic(err)
		}

		// Insert new widget for that uses
		// the variant column value
		db.Create(&Widget{
			ID: iid,
			Item: vitm
		})

		// Read it back
		var widget Widget
		db.First(&widget, "\"ID\" = ?", iid) // find new widget

		// extract item from widget
		if err = widget.Item.Get(&itm); err == nil {
			// and done
			fmt.Println(itm)
			}
		}
/*/

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	// db driver
	_ "github.com/snowflakedb/gosnowflake"
)

// Variant Showflake mapping data type
type Variant struct {
	value []byte
}

// Value returns a proper value to
// for writing to db
func (o Variant) Value() (driver.Value, error) {
	return fmt.Sprintf("%s", o.value), nil
}

// Scan scan value into Variant
func (o *Variant) Scan(value interface{}) (err error) {

	if v, ok := value.(string); ok {
		o.value = []byte(v)
	} else {
		o.value, err = json.Marshal(value)
	}
	return
}

// NewVariant create a new Variant type from GO
// stuct or array. Error returned if not JSON 
// parsable 
func NewVariant(v interface{}) (Variant, error) {
	var o Variant
	err := o.Set(v)
	return o, err
}

// Set loads a new type into the Variant wrapper
func (o *Variant) Set(value interface{}) (err error) {
	o.value, err = json.Marshal(value)
	return
}

// Get extracts the innner struct as an interface
func (o Variant) Get(value interface{}) (err error) {
	err = json.Unmarshal(o.value, value)
	return
}

// String returns string representation of value
func (o Variant) String() string {
	return fmt.Sprintf("%s", o.value)
}

// Object is an alias for
// the snowflake Variant type.
type Object Variant

// NewObject returns a new Variant that
// can manage structs, maps and arrays
func NewObject(v interface{}) (Variant, error) {
	return NewVariant(v)
}

// Array is an alias for the
// snowflake Variant type
type Array Variant

// NewArray returns a new Variant that
// can manage structs, maps and arrays
func NewArray(v interface{}) (Variant, error) {
	return NewVariant(v)
}
