package engine

func (suite *EngineSuite) TestInfo() {
	suite.RunScript(`
CREATE TABLE myTable (
	col1 TEXT,
	col2 INTEGER
);
INSERT INTO myTable VALUES
	("hello", 1),
	("world", 2)
`)

	info, err := suite.engine.Info()
	suite.NoError(err)

	expectedTableInfo := TableInfo{
		Page:      3,
		RowAmount: 2,
		Name:      "myTable",
	}

	suite.EqualValues([]string{"myTable"}, info.TableNames())
	suite.Len(info.Tables(), 1)
	suite.Equal(expectedTableInfo, info.Tables()[0])

	tbl, ok := info.Table("myTable")
	suite.True(ok)
	suite.Equal(expectedTableInfo, tbl)
}
