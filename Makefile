SHELL=/bin/bash

.PHONY: build

build:
	gb build

test:
	gb test

version: build
	bin/dogestry version
