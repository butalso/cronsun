package genid

import (
	"encoding/hex"

	"github.com/rogpeppe/fastuuid"
)

var generator *fastuuid.Generator

func init()  {
	var err error
	generator, err = fastuuid.NewGenerator()
	if err != nil {
		panic(err)
	}
}

func NextID() string {
	id := generator.Next()
	return hex.EncodeToString(id[:])
}
