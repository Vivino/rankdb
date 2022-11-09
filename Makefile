COMMIT := $(shell git rev-parse HEAD)
COMMIT_SHORT := $(shell git rev-parse HEAD | cut -c -7)
PROJECT := github.com/Vivino/rankdb
BUILD_ARCH :=
export GO111MODULE = on

all: test build

test: clean install
	go test ./...
	go test -race -short .

fulltest: clean install
	go test -cover ./...
	go test -race -short ./...

lint:
	golint ./...
	go vet ./...

clean:
	rm bin/go-ranks || true
	rm bin/go-ranks-cli || true

generate: install
	go install github.com/tinylib/msgp
	go install github.com/goadesign/goa/goagen
	go install golang.org/x/tools/cmd/goimports
	go generate ${PROJECT}
	go generate ${PROJECT}/api
	goimports -w .
	gofmt -s -w ./

run: build
	mkdir -p db/
	./bin/go-ranks -config=./conf/conf.toml

build: install
	mkdir -p bin/
	${BUILD_ARCH} go build -ldflags "-X ${PROJECT}/api.gitcommit=${COMMIT}" -o bin/go-ranks  ${PROJECT}/cmd/rankdb
	${BUILD_ARCH} go build -o bin/go-ranks-cli  ${PROJECT}/api/tool/rankdb-cli

dist: build
	go install github.com/goreleaser/goreleaser
	goreleaser --snapshot --skip-publish --rm-dist

install:
	@echo "Pre-compiling"
	go install -v ${PROJECT}/...
	go get -u golang.org/x/lint/golint

commit:
	@echo ${COMMIT_SHORT}

push-image:
	docker push ${IMAGE}:${VERSION}
	docker push ${IMAGE}:latest
