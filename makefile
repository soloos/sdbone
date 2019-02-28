SWAL_LDFLAGS += -X "soloos/sdbone/version.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
SWAL_LDFLAGS += -X "soloos/sdbone/version.GitHash=$(shell git rev-parse HEAD)"
# SWAL_PREFIX += GOTMPDIR=./go.build/tmp GOCACHE=./go.build/cache

SOLOOS_SWAL_PROTOS = $(shell find lib/soloos/sdbone -name '*.fbs')
GENERATED_PROTOS = $(shell find lib/soloos/sdbone -name "*.fbs"| sed 's/\.fbs/\.fbs\.go/g')
SOURCES = $(shell find . -name '*.go') $(GENERATED_PROTOS)

%.fbs.go: $(SOLOOS_SWAL_PROTOS)
	flatc -o ./lib/soloos/sdbone -g $(SOLOOS_SWAL_PROTOS)

fbs: $(GENERATED_PROTOS)

all:sdboned
sdboned:
	$(SWAL_PREFIX) go build -i -ldflags '$(SWAL_LDFLAGS)' -o ./bin/sdboned sdboned

include ./make/test
include ./make/bench

.PHONY:all soloos-server test
