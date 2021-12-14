



venv: requirements.txt
	virtualenv venv
	venv/bin/pip install --upgrade pip
	venv/bin/python -m pip install -r requirements.txt

test:
	-rm -rf analysis
	-rm -rf forecast
	-rm -rf reforecast
	venv/bin/python parse.py


develop: setup.py
	$(info ********* REMOVE AND REINSTALL PY PACKAGE *********)
	python setup.py clean --all && \
	python setup.py develop
