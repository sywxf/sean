package core

// 表列
type Column struct {
	CName    string
	CType    string
	CNoNull  bool
	CDefault string
	CUnique  bool
}
