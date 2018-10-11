package mfetl

import (
	"github.com/myfantasy/mfe"
)

// RunMethod Run methods conf param: method
func RunMethod(conf mfe.Variant) (err error) {
	method := conf.GE("method").Str()

	if method == "copy" {
		return CopyTable(conf)
	}

	return nil

}
