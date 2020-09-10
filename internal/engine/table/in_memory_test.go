package table

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/xqueries/xdb/internal/engine/types"
)

func TestInMemorySuite(t *testing.T) {
	suite.Run(t, new(InMemorySuite))
}

type InMemorySuite struct {
	TableSuite

	table inMemoryTable
}

func (suite *InMemorySuite) SetupTest() {
	suite.table = NewInMemory([]Col{
		{QualifiedName: "col1", Alias: "", Type: types.Integer},
		{QualifiedName: "col2", Alias: "", Type: types.Date},
		{QualifiedName: "col3", Alias: "", Type: types.String},
		{QualifiedName: "col4", Alias: "", Type: types.String},
		{QualifiedName: "col5", Alias: "", Type: types.Date},
		{QualifiedName: "col6", Alias: "", Type: types.Date},
		{QualifiedName: "col7", Alias: "", Type: types.Bool},
		{QualifiedName: "col8", Alias: "", Type: types.String},
		{QualifiedName: "col9", Alias: "", Type: types.Real},
	}, []Row{
		{Values: []types.Value{types.NewInteger(5577006791947779410), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(8674665223082153551), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(6129484611666145821), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(4037200794235010051), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(3916589616287113937), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(6334824724549167320), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(605394647632969758), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(1443635317331776148), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(894385949183117216), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(2775422040480279449), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(4751997750760398084), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(7504504064263669287), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(1976235410884491574), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(3510942875414458836), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(2933568871211445515), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(4324745483838182873), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(2610529275472644968), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
		{Values: []types.Value{types.NewInteger(2703387474910584091), types.NewDate(time.Now()), types.NewString("abc"), types.NewString("def"), types.NewDate(time.Now()), types.NewDate(time.Now()), types.NewBool(false), types.NewString("foobar"), types.NewReal(12.79)}},
	}).(inMemoryTable)
}

func (suite *InMemorySuite) TestInMemoryTable_Rows() {
	it1, err := suite.table.Rows()
	suite.NoError(err)
	it2, err := suite.table.Rows()
	suite.NoError(err)
	suite.NotSame(it1, it2)

}
