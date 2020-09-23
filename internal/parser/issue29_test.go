package parser

import "testing"

func TestIssue29(t *testing.T)  {
	NegativeTest{
		Name:  "issue29",
		Query: "CREATE TABLE(n,FOREIGN KEY()REFERENCES n ON DELETE CASCADE)",
	}.Run(t)
}

func TestIssue29WithoutTableName(t *testing.T) {
	NegativeTest{
		Name:  "issue29 without table name",
		Query: "CREATE TABLE (foo)",
	}.Run(t)
}

func TestIssue29WithNumericColumn(t *testing.T)  {
	NegativeTest{
		Name:  "issue29 with numeric column",
		Query: "CREATE TABLE foo(1)",
	}.Run(t)
}

func TestIssue29WithNumericTable(t *testing.T)  {
	NegativeTest{
		Name:  "issue29 with numeric column",
		Query: "CREATE TABLE 1(foo)",
	}.Run(t)
}
