



venv: requirements.txt
	virtualenv venv
	venv/bin/pip install --upgrade pip
	venv/bin/python -m pip install -r requirements.txt

test:
	-rm -rf analysis
	-rm -rf forecast
	-rm -rf reforecast
	venv/bin/python parse.py


clean:
	aws s3 rm s3://euppparquet-analysis/ --recursive
upload: clean
	#aws s3 cp analysis.parquet s3://euppparquet-analysis/ --recursive
	aws s3 sync analysis.parquet s3://euppparquet-analysis/

develop: setup.py
	$(info ********* REMOVE AND REINSTALL PY PACKAGE *********)
	python setup.py clean --all && \
	python setup.py develop
