#!/bin/sh -eu

rm -rf dist
mkdir dist
export GO111MODULE="on" 
o build -o dist/marketo-service main.go
