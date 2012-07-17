clean:
	@find . -name '*.pyc' -delete

tests: clean
	@python run_tests.py
