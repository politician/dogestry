SHELL=/bin/bash

.PHONY: build

build:
	gb build

version: build
	bin/dogestry version
