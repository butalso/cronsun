package conf

import "testing"

func TestInit(t *testing.T) {
	Init("conf/files/base.json", true)
}
