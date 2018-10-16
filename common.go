package mfetl

import (
	"errors"

	"github.com/myfantasy/mfe"
)

// RunMethods Run methods conf param: method
type RunMethods struct {
	Funcs map[string]func(conf mfe.Variant) (err error)
}

// RunByName methods by methodName
func (rm RunMethods) RunByName(methodName string, conf mfe.Variant) (err error) {
	method, d := rm.Funcs[conf.GE(methodName).Str()]

	if d {
		return method(conf)
	}
	return errors.New("Method not found")
}

// Run methods conf param: method
func (rm RunMethods) Run(conf mfe.Variant) (err error) {
	method, d := rm.Funcs[conf.GE("method").Str()]

	if d {
		return method(conf)
	}
	return errors.New("Method not found")
}

// CreateRunMethods create RunMethods with standart funcs
func CreateRunMethods() (rm RunMethods) {
	rm.Funcs["copy"] = CopyTable
	rm.Funcs["table_queue"] = TableQueue

	return rm
}
