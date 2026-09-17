PYTHON ?= python3

.PHONY: setup seed gen silver gold dq test clean
setup:
	$(PYTHON) -m pip install --break-system-packages -r requirements.txt
seed:
	$(PYTHON) tools/seed_demo_dims.py
gen:
	$(PYTHON) tools/generator.py --vehicles 200 --minutes 60 --out data/bronze/telemetry
silver:
	$(PYTHON) jobs/silver_job.py
gold:
	$(PYTHON) jobs/gold_job.py --business-date 2026-09-01
distributor:
	$(PYTHON) jobs/distributor_monthly_job.py --input data/demo_distributor_sales.csv --month 2026-09
dashboard:
	$(PYTHON) bi/serve_dashboard.py --port 8050
test:
	$(PYTHON) -m pytest -v
demo: clean seed gen silver gold distributor test
clean:
	rm -rf data
