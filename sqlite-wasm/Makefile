SHELL := /bin/bash

# bedrock sqlite source from @rocicorp/zero-sqlite3
ZERO_SQLITE3_DIR ?= ../node_modules/@rocicorp/zero-sqlite3/deps/sqlite3
SQLITE_SRC_FILES = sqlite-src/sqlite3.c sqlite-src/sqlite3.h

JS_PRE_FILES = native/api.js native/vfs-pre.js
JS_LIB_FILES = native/vfs.js
OBJECT_FILES = build/sqlite3.o build/vfs.o
EXPORTED_FUNCS_JSON = build/exp_funcs.json

# compile flags from @rocicorp/zero-sqlite3 defines.gypi
# adapted for wasm: THREADSAFE=0 (single-threaded), OS_OTHER=1, TEMP_STORE=3
SQLITE_FLAGS = \
	-DSQLITE_DEFAULT_CACHE_SIZE=-16000 \
	-DSQLITE_DEFAULT_FOREIGN_KEYS=1 \
	-DSQLITE_DEFAULT_MEMSTATUS=0 \
	-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 \
	-DSQLITE_DQS=0 \
	-DSQLITE_ENABLE_COLUMN_METADATA \
	-DSQLITE_ENABLE_DBSTAT_VTAB \
	-DSQLITE_ENABLE_DESERIALIZE \
	-DSQLITE_ENABLE_FTS3 \
	-DSQLITE_ENABLE_FTS3_PARENTHESIS \
	-DSQLITE_ENABLE_FTS4 \
	-DSQLITE_ENABLE_FTS5 \
	-DSQLITE_ENABLE_GEOPOLY \
	-DSQLITE_ENABLE_JSON1 \
	-DSQLITE_ENABLE_MATH_FUNCTIONS \
	-DSQLITE_ENABLE_RTREE \
	-DSQLITE_ENABLE_STAT4 \
	-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT \
	-DSQLITE_LIKE_DOESNT_MATCH_BLOBS \
	-DSQLITE_OMIT_DEPRECATED \
	-DSQLITE_OMIT_LOAD_EXTENSION \
	-DSQLITE_OMIT_PROGRESS_CALLBACK \
	-DSQLITE_OMIT_SHARED_CACHE \
	-DSQLITE_OMIT_TCL_VARIABLE \
	-DSQLITE_SOUNDEX \
	-DSQLITE_THREADSAFE=0 \
	-DSQLITE_TRACE_SIZE_LIMIT=32 \
	-DSQLITE_USE_URI=0 \
	-DSQLITE_TEMP_STORE=3 \
	-DSQLITE_OS_OTHER=1

EM_FLAGS = -O3 -flto

LINK_FLAGS = \
	-s EXPORTED_FUNCTIONS=@$(EXPORTED_FUNCS_JSON) \
	-s EXPORTED_RUNTIME_METHODS=cwrap,addFunction,removeFunction \
	-s NODEJS_CATCH_EXIT=0 \
	-s NODEJS_CATCH_REJECTION=0 \
	-s MODULARIZE=0 \
	-s ALLOW_TABLE_GROWTH=1 \
	-s ALLOW_MEMORY_GROWTH=1 \
	-s ENVIRONMENT=node \
	-s FILESYSTEM=0 \
	-s WASM_BIGINT \
	-s WASM_ASYNC_COMPILATION=0

all: dist/sqlite3.js dist/package.json

dist/sqlite3.js: $(OBJECT_FILES) $(EXPORTED_FUNCS_JSON) $(JS_PRE_FILES) $(JS_LIB_FILES)
	mkdir -p dist
	emcc $(LINK_FLAGS) $(EM_FLAGS) $(OBJECT_FILES) --js-library $(JS_LIB_FILES) \
		$(foreach f,$(JS_PRE_FILES),--pre-js $(f)) -o $@

# force CJS so __dirname is available for wasm file path resolution
dist/package.json:
	mkdir -p dist
	echo '{"type": "commonjs"}' > $@

build/sqlite3.o: $(SQLITE_SRC_FILES)
	mkdir -p build
	emcc $(EM_FLAGS) $(SQLITE_FLAGS) -c $< -o $@

build/vfs.o: native/vfs.c sqlite-src/sqlite3.h
	mkdir -p build
	emcc $(EM_FLAGS) $(SQLITE_FLAGS) -I sqlite-src -c $< -o $@

$(EXPORTED_FUNCS_JSON): native/api.js
	mkdir -p build
	echo '[' > $@
	perl -p0e 's/.*signatures = \{\n(.+?)\s*\}.*/\1/smg' native/api.js | \
		perl -pe 's/\s*(.+):.*/"_sqlite3_\1"/' | \
		paste -sd "," - >> $@
	echo ',"_malloc","_free"]' >> $@

# copy bedrock sqlite source from zero-sqlite3
$(SQLITE_SRC_FILES):
	mkdir -p sqlite-src
	cp $(ZERO_SQLITE3_DIR)/sqlite3.c sqlite-src/
	cp $(ZERO_SQLITE3_DIR)/sqlite3.h sqlite-src/

.PHONY: clean
clean:
	rm -rf build dist sqlite-src

.PHONY: docker
docker:
	docker run --rm -v $$(pwd)/..:$$(pwd)/.. -w $$(pwd) -u $$(id -u):$$(id -g) \
		emscripten/emsdk:5.0.0 make all
