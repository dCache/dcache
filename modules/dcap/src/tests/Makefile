TEST_DIRS := uri

TEST_TESTS = $(TEST_DIRS:%=%__TEST__)
CLEAN_TESTS = $(TEST_DIRS:%=%__CLEAN__)

all: test

test: $(TEST_TESTS)

clean: $(CLEAN_TESTS)

$(TEST_TESTS):
	@make -C $(@:%__TEST__=%) test

$(CLEAN_TESTS):
	@make -C $(@:%__CLEAN__=%) clean

.PHONY: $(TEST_TESTS) $(CLEAN_TESTS) all test clean

