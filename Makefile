EXTENSION  = pg_dbms_job
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
		sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

PGFILEDESC = "pg_dbms_job - Propose Oracle DBMS_JOB compatibility for PostgreSQL"

PG_CONFIG = pg_config
PG91 = $(shell $(PG_CONFIG) --version | egrep " 8\.| 9\.0" > /dev/null && echo no || echo yes)

ifeq ($(PG91),yes)
DOCS = $(wildcard README*)
MODULES =

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
else
$(error Minimum version of PostgreSQL required is 9.1.0)
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

install: distconf rust-install

distconf:
	install -D --mode=600 --owner=postgres etc/$(EXTENSION).conf /etc/$(EXTENSION)/$(EXTENSION).conf.dist

# ---------------------------------------------------------------------------
# Rust scheduler daemon (rust/). `make` and `make install` build and install
# the pg_dbms_job binary alongside the SQL extension; the rust-* targets can
# also be invoked on their own. Requires cargo (https://rustup.rs) — see
# rust/README.md. Override CARGO= to point at a specific toolchain.
# ---------------------------------------------------------------------------
CARGO ?= cargo
RUST_DIR = rust
RUST_BIN = $(RUST_DIR)/target/release/$(EXTENSION)

all: rust-build

rust-build:
	@if command -v $(CARGO) >/dev/null 2>&1; then \
	    echo "$(CARGO) build --release --manifest-path $(RUST_DIR)/Cargo.toml"; \
	    $(CARGO) build --release --manifest-path $(RUST_DIR)/Cargo.toml; \
	elif [ -x $(RUST_BIN) ]; then \
	    echo "NOTE: '$(CARGO)' not on PATH; using already-built $(RUST_BIN)"; \
	else \
	    echo "ERROR: '$(CARGO)' not found and $(RUST_BIN) is not built."; \
	    echo "       Install Rust (https://rustup.rs) and run 'make' first,"; \
	    echo "       or set CARGO=/path/to/cargo."; \
	    exit 1; \
	fi

rust-install: rust-build
	install -D --mode=755 $(RUST_BIN) $(DESTDIR)$(bindir)/$(EXTENSION)

clean: rust-clean

rust-clean:
	$(CARGO) clean --manifest-path $(RUST_DIR)/Cargo.toml

.PHONY: distconf rust-build rust-install rust-clean
