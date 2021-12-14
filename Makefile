



test:
	-rm -rf analysis
	-rm -rf forecast
	-rm -rf reforecast
	venv/bin/python parse.py
