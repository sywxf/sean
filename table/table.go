package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"strings"
	"time"
)

func Log_P(err ...interface{}) {
	log.Println(err...)
}

const (
	P_int       = "integer"
	P_numeric   = "numeric"
	P_real      = "real"
	P_text      = "text"
	P_timestamp = "timestamp"
)

// 表列
type column struct {
	CName    string
	CType    string
	CNoNull  bool
	CDefault string
	CUnique  bool
}

// 表定义
type TableModel struct {
	Name    string
	Columns []column
}

// 查询结果 map[string]string{"id": 1, "column_1": "value_1", "column_2": "value_2"}
type TableValue map[string]string

type pdb struct {
	dbcon        *sql.DB
	databasename string
	whereStr     string
	Params       []interface{}
	columns      []string
}

func NewPdb() *pdb {
	p := new(pdb)
	p.Init()
	return p
}

func (p *pdb) Init() {
	p.whereStr = ""
	p.Params = []interface{}{}
}

// 连接库
func (p *pdb) openDatabase(dbname string) error {
	if p.databasename == dbname {
		return nil
	}
	// constr := fmt.Sprintf("user=postgres password=windows dbname=%s sslmode=disable", dbname)
	constr := fmt.Sprintf("user=sywxf dbname=%s sslmode=disable", dbname)
	db, err := sql.Open("postgres", constr)
	if err == nil {
		p.dbcon = db
		p.databasename = dbname
	}
	Log_P(err, "open database", dbname)
	return err
}

// 建库
func (p *pdb) createDb(dbname string) error {
	execstr := fmt.Sprintf("CREATE DATABASE %s;", dbname)
	_, err := p.dbcon.Exec(execstr)
	Log_P(err, execstr)
	return err
}

// 删库
func (p *pdb) RemoveDb(dbname string) error {
	execstr := fmt.Sprintf("DROP DATABASE %s;", dbname)
	_, err := p.dbcon.Exec(execstr)
	Log_P(err, execstr)
	return err
}

// 建表
func (p *pdb) CreateTable(t *TableModel) error {
	tableColumns := ""
	for _, c := range t.Columns {
		tableColumns = tableColumns + c.CName + " " + c.CType
		if c.CNoNull {
			tableColumns = tableColumns + " " + "NOT NULL"
		}
		if c.CUnique {
			tableColumns = tableColumns + " " + "UNIQUE"
		}
		if c.CDefault != "" {
			tableColumns = tableColumns + " DEFAULT " + c.CDefault
		}
		tableColumns = tableColumns + ","
	}
	execstr := fmt.Sprintf("CREATE TABLE %s (tid serial, %s created timestamp DEFAULT now());", t.Name, tableColumns)
	_, err := p.dbcon.Exec(execstr)
	Log_P(err, execstr)
	return err
}

// 初始化账套库
func (p *pdb) InitOwnDb() error {
	Log_P("===== Init start =====")
	err := p.openDatabase("postgres")
	if err != nil {
		return err
	}
	dbname := "own"
	err = p.createDb(dbname)
	if err != nil {
		return err
	}
	err = p.openDatabase(dbname)
	if err != nil {
		return err
	}
	err = p.CreateTable(&TableModel{
		"databasenames", []column{
			column{CName: "name", CType: P_text, CNoNull: true, CDefault: "", CUnique: true},
			column{CName: "real_name", CType: P_text, CNoNull: true, CDefault: "", CUnique: true}}})
	if err != nil {
		return err
	}
	Log_P("===== Init end =====")
	return nil
}

// 建账
func (p *pdb) CreateAccount(name string) error {
	Log_P("===== Create Account =====")
	p.dbcon.Close()
	err := p.openDatabase("own")
	if err != nil {
		return err
	}
	accountName := fmt.Sprintf("account_%s", getSuffString())
	err = p.createDb(accountName)
	if err != nil {
		Log_P("Create Account error:", err)
		return err
	}
	//insert into table
	contextStr := fmt.Sprintf("insert into databasenames (name,real_name)values('%s','%s')", accountName, name)
	_, err = p.dbcon.Exec(contextStr)
	if err != nil {
		Log_P("Add Account error:", err, contextStr)
		//remove db
		p.RemoveDb(accountName)
		return err
	}
	Log_P("===== Create Account end =====")
	return nil
}

// 后缀
func getSuffString() string {
	now := time.Now()
	year, mon, day := now.Local().Date()
	hour, min, sec := now.Local().Clock()
	return fmt.Sprintf("%d%02d%02d%02d%02d%02d", year, mon, day, hour, min, sec)
}

// 查找账套
func (p *pdb) GetAccounts() []map[string]string {
	err := p.openDatabase("own")
	if err != nil {
		return nil
	}
	result, err := p.Find([]string{"databasenames"}, "*")
	if err != nil {
		Log_P("Get Accounts error:", err)
		return nil
	}
	return result
}

func (p *pdb) Where(whereStr string, Params ...interface{}) *pdb {
	p.whereStr = whereStr
	p.Params = Params
	return p
}

func (p *pdb) Find(tableNames []string, columns ...string) ([]map[string]string, error) {
	defer p.Init()
	result := []map[string]string{}
	contextStr := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ","), strings.Join(tableNames, ","))
	if p.whereStr != "" {
		contextStr = contextStr + " WHERE " + p.whereStr
	}
	Log_P(contextStr, p.Params)
	rows, err := p.dbcon.Query(contextStr, p.Params...)
	defer rows.Close()
	if err != nil {
		Log_P("Query error", err, contextStr)
		return nil, err
	}
	// Get column names
	r_columns, err := rows.Columns()
	if err != nil {
		Log_P("row.Columns error:", err)
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
			Log_P("row.Scan error:", err)
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
		Log_P("rows error:", err)
		return nil, err
	}
	return result, nil
}

// 查找库内所有表
func (p *pdb) FindAllTableInDatabase(dbname string) []map[string]string {
	err := p.openDatabase(dbname)
	if err != nil {
		return nil
	}
	tables, err := p.Where("schemaname=$1", "public").Find([]string{"pg_tables"}, "tablename")
	if err != nil {
		return nil
	}
	return tables
}

func (p *pdb) FindAllColumnInTable(tablename string) []map[string]string {
	columns, err := p.Where("table_name = $1", tablename).Find([]string{"INFORMATION_SCHEMA.COLUMNS"}, "column_name", "column_default", "is_nullable", "data_type", "character_maximum_length",
		"numeric_precision", "numeric_precision_radix")
	if err != nil {
		return nil
	}
	return columns
}

// 插入记录
func (p *pdb) Insert(tableName string, columns map[string]string) (sql.Result, error) {
	index := 1
	values, cols, args := []string{}, []string{}, []interface{}{}
	for k, v := range columns {
		values = append(values, fmt.Sprintf("$%d", index))
		cols = append(cols, k)
		args = append(args, v)
		index++
	}
	contextStr := fmt.Sprintf("insert into %s(%s)values(%s)", tableName, strings.Join(cols, ","), strings.Join(values, ","))
	stmt, err := p.dbcon.Prepare(contextStr)
	if err != nil {
		Log_P("Insert Prepare error:", err, contextStr, args)
		return nil, err
	}
	rs, err := stmt.Exec(args...)
	if err != nil {
		Log_P("Insert Exec error:", err)
		return nil, err
	}
	return rs, nil
}

func main() {
	pc := NewPdb()
	// pc.InitOwnDb()
	// pc.CreateAccount("第一个账套")
	// pc.CreateAccount("第二个账套")
	// a := pc.GetAccounts()
	// log.Println(a)
	pc.openDatabase("own")
	// b, _ := pc.Find([]string{"databasenames"}, "name", "real_name", "tid")
	// log.Println(b)

	// c, _ := pc.Where("schemaname=$1", "public").Find([]string{"pg_tables"}, "*")
	// log.Println(c)

	// d := pc.FindAllTableInDatabase("own")
	// log.Println(d)

	e := pc.FindAllColumnInTable("databasenames")
	log.Println(e)

	// f, _ := pc.Insert("databasenames", map[string]string{"name": "def", "real_name": "hkj"})
	// log.Println(f)

	pc.CreateTable(&TableModel{
		"typetest", []column{
			column{CName: "name", CType: P_text, CNoNull: true, CDefault: "", CUnique: true},
			column{CName: "numbers", CType: P_real, CNoNull: true, CDefault: "0.0", CUnique: false},
			column{CName: "price", CType: P_numeric, CNoNull: false, CDefault: "", CUnique: false},
			column{CName: "confirm", CType: P_timestamp, CNoNull: false, CDefault: "", CUnique: false},
		}})

	pc.Insert("typetest", map[string]string{"name": "nametest", "numbers": "123.5645", "price": "5646.46", "confirm": "2015-01-01"})
	pc.dbcon.Close()
}
