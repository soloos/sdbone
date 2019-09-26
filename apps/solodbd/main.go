package main

import (
	"os"
	"soloos/common/util"
	"soloos/solodb/solodbd"
)

func main() {
	var (
		solodbdIns solodbd.SOLODBD
		options    solodbd.Options
		err        error
	)

	optionsFile := os.Args[1]

	err = util.LoadOptionsFile(optionsFile, &options)
	util.AssertErrIsNil(err)

	util.AssertErrIsNil(solodbdIns.Init(options))
	util.AssertErrIsNil(solodbdIns.Start())
}
