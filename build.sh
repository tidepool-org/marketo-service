#!/bin/sh -eu

rm -rf dist
mkdir dist
export GO111MODULE="on" 
go build -o dist/marketo-service main.go