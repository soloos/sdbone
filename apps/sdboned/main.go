package main

import (
	"os"
	"soloos/common/util"
	"soloos/sdbone/sdboned"
)

func main() {
	var (
		sdbonedIns sdboned.SDBONED
		options    sdboned.Options
		err        error
	)

	optionsFile := os.Args[1]

	err = util.LoadOptionsFile(optionsFile, &options)
	util.AssertErrIsNil(err)

	util.AssertErrIsNil(sdbonedIns.Init(options))
	util.AssertErrIsNil(sdbonedIns.Start())
}
