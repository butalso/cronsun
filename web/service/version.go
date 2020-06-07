package service

import (
	"fmt"
	"runtime"
)

const VersionNumber = "0.3.5"

var (
	version = fmt.Sprintf("v%s (build %s)", VersionNumber, runtime.Version())
)

func GetVersion(ctx *Context) {
	outJSON(ctx.W, version)
}