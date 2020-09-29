#!/bin/sh -eu

rm -rf dist
mkdir dist
export GO111MODULE="on" 
go build -gcflags="all=-N -l" -o dist/marketo-service main.go
