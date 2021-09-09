TESTS=

install:
	@pipenv --bare install --dev

shell: install
	@pipenv --bare shell

test: install
	@pipenv run python3 -m unittest discover --start-directory ./test --catch --failfast -k=$(TESTS)

clean:
	@pipenv --bare --rm

.PHONY: clean install shell test
