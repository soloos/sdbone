SOLOMQ_LDFLAGS += -X "soloos/solodb/version.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
SOLOMQ_LDFLAGS += -X "soloos/solodb/version.GitHash=$(shell git rev-parse HEAD)"
# SOLOMQ_PREFIX += GOTMPDIR=./go.build/tmp GOCACHE=./go.build/cache

SOLOOS_SOLOMQ_PROTOS = $(shell find ./ -name '*.fbs'|grep -v vendor)
GENERATED_PROTOS = $(shell find ./ -name "*.fbs"|grep -v vendor| sed 's/\.fbs/\.fbs\.go/g')
SOURCES = $(shell find . -name '*.go') $(GENERATED_PROTOS)

%.fbs.go: $(SOLOOS_SOLOMQ_PROTOS)
	flatc -o ./ -g $(SOLOOS_SOLOMQ_PROTOS)

fbs: $(GENERATED_PROTOS)

all:solodbd
solodbd:
	$(SOLOMQ_PREFIX) go build -i -ldflags '$(SOLOMQ_LDFLAGS)' -o ./bin/solodbd ./apps/solodbd

include ./make/test
include ./make/bench

.PHONY:all solodbd test
