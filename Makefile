CMAKE ?= $(shell command -v cmake 2>/dev/null || echo .venv/bin/cmake)
JOBS  ?= 8

.PHONY: build test asan bench run clean

build:
	$(CMAKE) -S . -B build -DCMAKE_BUILD_TYPE=Release
	$(CMAKE) --build build -j $(JOBS)

test: build
	./build/matchbox_tests

asan:
	$(CMAKE) -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DMATCHBOX_ASAN=ON
	$(CMAKE) --build build-asan -j $(JOBS)
	./build-asan/matchbox_tests

bench: build
	./build/matchbox_bench --orders 1000000

run: build
	./build/matchbox_server

clean:
	rm -rf build build-asan
