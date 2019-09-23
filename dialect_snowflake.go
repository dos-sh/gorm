package gorm

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// init registers the dialect with gorm
func init() {
	RegisterDialect("snowflake", &snowflake{})
}

// snowflake dialect
type snowflake struct {
	commonDialect
}

// GetName returns name of dialect
func (snowflake) GetName() string {
	return "snowflake"
}

// CurrentDatabase returns name of currently connnected database
func (s snowflake) CurrentDatabase() string {
	var dbName string
	if err := s.db.QueryRow("SELECT CURRENT_DATABASE()").Scan(&dbName); err != nil {
		return ""
	}
	return dbName
}

func (snowflake) StructsAreNormal() bool {
	return true
}

// CurrentSchema returns name of schema currently in use
func (s snowflake) CurrentSchema() string {
	var schema string
	if err := s.db.QueryRow("SELECT CURRENT_SCHEMA()").Scan(&schema); err != nil {
		return ""
	}

	return schema
}



// HasTable checks current db/schema for table existance
func (s snowflake) HasTable(tableName string) bool {
	var count int

	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema=? and table_name=?",
		s.CurrentSchema(),
		tableName,
	).Scan(&count)

	return count > 0
}

// HasColummn checks to see if columm exists in table
func (s snowflake) HasColumn(tableName string, columnName string) bool {
	var count int

	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? AND column_name = ?",
		s.CurrentSchema(),
		tableName,
		columnName,
	).Scan(&count)
	return count > 0
}

// HasIndex checks for existing index
func (s snowflake) HasIndex(tableName string, indexName string) bool {
	var count int

	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema = ? AND table_name = ? AND index_name = ?",
		s.CurrentSchema(),
		tableName,
		indexName,
	).Scan(&count)
	return count > 0
}

// DataTypeOf translates Go types to SQL types via reflection for table creation
func (s *snowflake) DataTypeOf(field *StructField) string {
	fmt.Println("Yelp")
	var dataValue, sqlType, size, additionalType = ParseFieldStructForDialect(field, s)
	fmt.Printf("Type %v: %v\n", dataValue.Kind(), dataValue)
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
			fmt.Println("Struct!")
			if _, ok := dataValue.Interface().(time.Time); ok {
				sqlType = "TIMESTAMP_TZ"
			} else {
				sqlType = "VARIANT"
			}
		case reflect.Map:
			sqlType = "VARIANT"
		default:
			if IsArrayOrSlice(dataValue) { 
				sqlType = "VARIANT"
				if IsByteArrayOrSlice(dataValue) {
					sqlType = "BINARY"

					if isJSON(dataValue) {
						sqlType = "VARIANT"
					}
				} 
			}
		}
	}

	fmt.Printf("SQL TYPE: %s\n", sqlType)
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

func (snowflake) InsertValuesModifier(fields []*Field) string {
	var fmtStr, colSelector string
	for i, fld := range fields {
		if colSelector != "" { 
			colSelector += ","
		}

		if t := fmt.Sprintf("%s\n",fld.StructField.Struct.Type); strings.Contains(t, "snowflake") {
			fmtStr = "%sparse_json(column%d)"
		} else {
			fmtStr = "%scolumn%d"
		}
		colSelector = fmt.Sprintf(fmtStr, colSelector, i+1)
	}

	return fmt.Sprintf("select %s from", colSelector)
} 

// IsArrayOrSlice returns true of the reflected value is an array or slice
func IsArrayOrSlice(value reflect.Value) bool {
	return (value.Kind() == reflect.Array || value.Kind() == reflect.Slice)
}
