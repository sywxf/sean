package core

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"strconv"
	"strings"
)

type Engine struct {
	dbcon        *sql.DB
	databaseName string
	whereStr     string
	params       []interface{}
	columns      []string
}

func NewEngine(conStr string) (*Engine, error) {
	db, err := sql.Open("postgres", conStr)
	if err != nil {
		return nil, err
	}
	return &Engine{
		dbcon:        db,
		databaseName: "",
		whereStr:     "",
	}, nil
}

func (e *Engine) Init() {
	e.whereStr = ""
	e.params = []interface{}{}
}

func (e *Engine) CreateDb(dbname string) error {
	execstr := fmt.Sprintf("CREATE DATABASE %s;", dbname)
	_, err := e.dbcon.Exec(execstr)
	return err
}

func (e *Engine) GetTables() ([]*Table, error) {
	args := []interface{}{}
	s := "SELECT tablename FROM pg_tables where schemaname = 'public'"

	rows, err := e.dbcon.Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]*Table, 0)
	for rows.Next() {
		table := NewEmptyTable()
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		table.Name = name
		tables = append(tables, table)
	}
	return tables, nil
}

func (e *Engine) Where(whereStr string, params ...interface{}) *Engine {
	e.whereStr = whereStr
	e.params = params
	return e
}

func (e *Engine) Find(tableNames []string, columns ...string) ([]map[string]string, error) {
	defer e.Init()
	result := []map[string]string{}
	contextStr := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ","), strings.Join(tableNames, ","))
	if e.whereStr != "" {
		contextStr = contextStr + " WHERE " + e.whereStr
	}
	rows, err := e.dbcon.Query(contextStr, e.params...)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	// Get column names
	r_columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	// Make a slice for the values
	values := make([]interface{}, len(r_columns))
	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	// loop
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		var r = map[string]string{}
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col == nil {
				r[r_columns[i]] = "NULL"
			} else {
				switch col.(type) {
				case int64:
					r[r_columns[i]] = fmt.Sprintf("%d", col)
					break
				case bool:
					r[r_columns[i]] = fmt.Sprintf("%v", col)
					break
				default:
					r[r_columns[i]] = fmt.Sprintf("%s", col)
					break
				}

			}
		}
		result = append(result, r)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) GetColumns(tableName string) ([]string, []*Column, error) {
	args := []interface{}{tableName}
	s := `SELECT column_name, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_precision_radix ,
    CASE WHEN p.contype = 'p' THEN true ELSE false END AS primarykey,
    CASE WHEN p.contype = 'u' THEN true ELSE false END AS uniquekey
    FROM pg_attribute f
    JOIN pg_class c ON c.oid = f.attrelid JOIN pg_type t ON t.oid = f.atttypid
    LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)
    LEFT JOIN pg_class AS g ON p.confrelid = g.oid
    LEFT JOIN INFORMATION_SCHEMA.COLUMNS s ON s.column_name=f.attname AND c.relname=s.table_name
    WHERE c.relkind = 'r'::char AND c.relname = $1 AND f.attnum > 0 ORDER BY f.attnum;`

	rows, err := e.dbcon.Query(s, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// tables := make([]*Table, 0)
	for rows.Next() {
		var colName, isNullable, dataType string
		var maxLenStr, colDefault, numPrecision, numRadix *string
		var isPK, isUnique bool
		err = rows.Scan(&colName, &colDefault, &isNullable, &dataType, &maxLenStr, &numPrecision, &numRadix, &isPK, &isUnique)
		if err != nil {
			return nil, nil, err
		}
		var maxLen int
		if maxLenStr != nil {
			maxLen, err = strconv.Atoi(*maxLenStr)
			if err != nil {
				return nil, nil, err
			}
		}
		var cd string
		if colDefault != nil {
			cd = *colDefault
		}
		var np string
		if numPrecision != nil {
			np = *numPrecision
		}
		var nr string
		if numRadix != nil {
			nr = *numRadix
		}

		fmt.Println(colName, cd, isNullable, dataType, maxLenStr, np, nr, isPK, isUnique, maxLen)
		// fmt.Printf("%d,%d,%d,%d\n", *maxLenStr, *colDefault, *numPrecision, *numRadix)
	}
	return nil, nil, nil
}
