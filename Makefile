PIPENV=pipenv --bare

TESTS=

install:
	@$(PIPENV) install --dev

shell: install
	@$(PIPENV) shell

documentation: install
	@$(PIPENV) run python3 docs/generate.py

test: install
	@$(PIPENV) run python3 -m unittest discover --verbose --start-directory ./test --catch --failfast -k=$(TESTS)

clean:
	@$(PIPENV) --rm

.PHONY: clean install shell documentation test
