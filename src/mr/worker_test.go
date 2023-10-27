package mr

import "testing"

func Test_ihash(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Test Case 1",
			args: args{"hello"},
			want: 12345, // 期望的结果
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ihash(tt.args.key); got != tt.want {
				t.Errorf("ihash() = %v, want %v", got, tt.want)
			}
		})
	}
}
