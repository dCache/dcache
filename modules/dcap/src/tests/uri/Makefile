TESTS_SRC := $(wildcard *.src)

TESTS_RESULTS = $(TESTS_SRC:%.src=%.actual)
TESTS_EXPECTED = $(TESTS_SRC:%.src=%.expected)
TESTS_OK = $(TESTS_SRC:%.src=%.ok)

CMP_CMD = cmp -s
URL_PARSE_CMD = ../../dcap_url-test

test: $(TESTS_OK)

clean:
	rm -f $(TESTS_OK) $(TESTS_RESULTS)

%.ok: %.actual %.expected
	@echo Testing $(<:%.actual=%)
	@$(CMP_CMD) $^ && touch $@

%.actual: %.src
	@$(URL_PARSE_CMD) $$(cat $<) > $@

.PHONY: test clean
.PRECIOUS: $(TESTS_RESULTS)

