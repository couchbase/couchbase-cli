PIPENV=pipenv --bare

TESTS=

install:
	@$(PIPENV) install --dev

shell: install
	@$(PIPENV) shell

test: install
	@$(PIPENV) run python3 -m unittest discover --start-directory ./test --catch --failfast -k=$(TESTS)

clean:
	@$(PIPENV) --rm

.PHONY: clean install shell test
