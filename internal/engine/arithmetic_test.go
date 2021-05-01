package engine

import (
	"github.com/xqueries/xdb/internal/engine/types"
)

func (suite *EngineSuite) TestAdd() {
	type args struct {
		left  types.Value
		right types.Value
	}
	tests := []struct {
		name    string
		args    args
		want    types.Value
		wantErr string
	}{
		{
			"nils",
			args{
				nil,
				nil,
			},
			nil,
			"cannot add <nil> and <nil>",
		},
		{
			"left nil",
			args{
				nil,
				types.NewInteger(5),
			},
			nil,
			"cannot add <nil> and types.IntegerValue",
		},
		{
			"right nil",
			args{
				types.NewInteger(5),
				nil,
			},
			nil,
			"cannot add types.IntegerValue and <nil>",
		},
		{
			"simple",
			args{
				types.NewInteger(5),
				types.NewInteger(6),
			},
			types.NewInteger(11),
			"",
		},
		{
			"zero",
			args{
				types.NewInteger(0),
				types.NewInteger(0),
			},
			types.NewInteger(0),
			"",
		},
		{
			"both negative",
			args{
				types.NewInteger(-5),
				types.NewInteger(-5),
			},
			types.NewInteger(-10),
			"",
		},
		{
			"left negative",
			args{
				types.NewInteger(-5),
				types.NewInteger(10),
			},
			types.NewInteger(5),
			"",
		},
		{
			"right negative",
			args{
				types.NewInteger(10),
				types.NewInteger(-5),
			},
			types.NewInteger(5),
			"",
		},
		{
			"overflow",
			args{
				types.NewInteger((1 << 63) - 1),
				types.NewInteger(5),
			},
			types.NewInteger(-(1 << 63) + 4),
			"",
		},
		{
			"negative overflow",
			args{
				types.NewInteger(-(1 << 63)),
				types.NewInteger(-1),
			},
			types.NewInteger((1 << 63) - 1),
			"",
		},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.engine.add(suite.ctx, tt.args.left, tt.args.right)
			if tt.wantErr != "" {
				suite.EqualError(err, tt.wantErr)
			} else {
				suite.NoError(err)
			}
			suite.Equal(tt.want, got)
		})
	}
}
