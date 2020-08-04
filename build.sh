#!/bin/sh -eu

rm -rf dist
mkdir dist
export GO111MODULE="on" 
go build -o dist/MarketoService main.go