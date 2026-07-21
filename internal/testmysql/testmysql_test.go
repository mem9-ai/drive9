package testmysql

import (
	"errors"
	"testing"
)

func TestIsPortBindConflict(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "docker ephemeral port race",
			err: errors.New("start container: Error response from daemon: failed to set up container networking: " +
				"driver failed programming external connectivity on endpoint foo (abc): " +
				"failed to bind host port for 0.0.0.0::172.18.0.2:3306/tcp: address already in use"),
			want: true,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "image pull failure",
			err:  errors.New("Error response from daemon: pull access denied for mysql:nope"),
			want: false,
		},
		{
			name: "address in use without bind context",
			err:  errors.New("listen tcp :3306: address already in use"),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPortBindConflict(tc.err); got != tc.want {
				t.Errorf("isPortBindConflict(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
