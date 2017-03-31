################################################################################
#
# March 2017
# Copyright (c) 2017 by cisco Systems, Inc.
# All rights reserved.
#
# Rudimentary build and test support
#
################################################################################

VERSION = $(shell git describe --always --long --dirty)

PKG = $(shell go list)

# If infra-utils package is not vendored in your workspace, (e.g. you
# are making changes to it, you can simply comment out the VENDOR
# line, and variable update on packages will assume they are under
# source.
VENDOR = $(PKG)/vendor/

LDFLAGS=-ldflags "-X  main.appVersion=v${VERSION}(bigmuddy)"

SOURCEDIR=.
SOURCES := $(shell find $(SOURCEDIR) -name '*.go' -o -name "*.proto" )
# Cumbersome way of excluding vendor directory
GO_BAR_VENDOR := $(shell go list ./... | egrep -v vendor/)

.DEFAULT_GOAL: bin/$(BINARY)

# Build binary in bin directory
bin/$(BINARY): $(SOURCES)
	@mkdir -p bin
	go vet $(GO_BAR_VENDOR)
	go fmt $(GO_BAR_VENDOR)
	go build $(LDFLAGS) -o bin/$(BINARY)

.PHONY: generated_source
generated_source:
	go generate -x

.PHONY: test
test:
	go test -v -run=. -bench=. $(PROFILE)

.PHONY: coverage
coverage: PROFILE=-coverprofile=test
coverage: test
	go tool cover -html=test

