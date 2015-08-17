package core

type Table struct {
	Name    string
	Columns []*Column
}

func NewTable(name string, columns []*Column) *Table {
	return &Table{
		Name:    name,
		Columns: columns,
	}
}

func NewEmptyTable() *Table {
	return NewTable("", nil)
}
