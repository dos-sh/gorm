package gorm

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"
)

// snowflake dialect
type snowflake struct {
	commonDialect
}

// init registers the dialect with gorm
func init() {
	RegisterDialect("snowflake", &snowflake{})
	log.Println("OK")
}

// GetName returns name of dialect
func (snowflake) GetName() string {
	return "snowflake"
}

// CurrentDatabase returns name of currently connnected database
func (s snowflake)CurrentDatabase() string {
	var dbName string
	if err := s.db.QueryRow("SELECT CURRENT_DATABASE()").Scan(&dbName); err != nil {
		return ""
	}
	return dbName
}

// HasTable checks current db/schema for table existance 
func (s snowflake) HasTable(tableName string) bool {
	var count int
	var schema string
	log.Println("Checking table")
	if err := s.db.QueryRow("SELECT CURRENT_SCHEMA()").Scan(&schema); err != nil {
		return false
	}
	
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema=? and table_name=?", schema, tableName).Scan(&count)
	return count > 0
}

// BindVar for snowflake is '?'
func (snowflake) BindVar(i int) string {
	return "?"
}

// DataTypeOf translates Go types to SQL types via reflection
func (s *snowflake) DataTypeOf(field *StructField) string {
	var dataValue, sqlType, size, additionalType = ParseFieldStructForDialect(field, s)

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "BOOLEAN"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uintptr:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "INTEGER AUTOINCREMENT"
			} else {
				sqlType = "INTEGER"
			}

		case reflect.Float32, reflect.Float64:
			sqlType = "NUMERIC"
		case reflect.String:
			if _, ok := field.TagSettingsGet("SIZE"); !ok {
				size = 0 // if SIZE haven't been set, use `text` as the default type, as there are no performance different
			}

			if size > 0 && size < 65532 {
				sqlType = fmt.Sprintf("VARCHAR(%d)", size)
			} else {
				sqlType = "TEXT"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				sqlType = "TIMESTAMP_TZ"
			} else {
				sqlType = "OBJECT" 
			}
		case reflect.Map:
				sqlType = "OBJECT"
		default:
			if IsByteArrayOrSlice(dataValue) {
				sqlType = "BINARY"

				if isJSON(dataValue) {
					sqlType = "OBJECT"
				}
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) for snowflake", dataValue.Type().Name(), dataValue.Kind().String()))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (snowflake) SupportLastInsertID() bool {
	return true
}
