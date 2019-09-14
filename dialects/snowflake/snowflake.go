package snowflake

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

// DB Type prereqs

// Value returns a proper value to 
// for writing to db
func (o Variant) Value() (driver.Value, error) {
	return fmt.Sprintf("%s", o.value), nil
}

// Scan scan value into Variant
func (o *Variant) Scan(value interface{}) error {	
	o.value = []byte(value.(string))
	return nil
}

// Dev API 

// NewVariant create a new Variant type
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
func (o Variant) Get(value interface{}) (interface{}, error) {
	err := json.Unmarshal(o.value, value)
	return value, err
}

// String returns string representation of value
func (o Variant) String() string {
	return fmt.Sprintf("%s", o.value)
}