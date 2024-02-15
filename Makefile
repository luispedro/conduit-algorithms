all: build

build:
	stack build $(STACKOPTS)

fast:
	stack build --fast --work-dir .stack-work-fast $(STACKOPTS)

check:
	stack test $(STACKOPTS)

fastcheck:
	stack test --fast --work-dir .stack-work-fast $(STACKOPTS)

# Synonym
tests: check

bench:
	stack bench --work-dir .stack-work $(STACKOPTS)

profile:
	stack build $(STACKOPTS) --executable-profiling --library-profiling --ghc-options="-fprof-auto -rtsopts"

clean:
	stack clean $(STACKOPTS)

distclean: clean

.PHONY: all build clean check tests distclean dist fast fastcheck bench
