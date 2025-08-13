# Arquivos de entrada e parâmetros padrão
DATA_GLOB   = "/data/*.csv"
PICKUP_COL  = tpep_pickup_datetime
DROPOFF_COL = tpep_dropoff_datetime
TIP_COL     = tip_amount

# Variáveis de Docker
DOCKER_RUN = docker compose run --rm -e PYTHONUNBUFFERED=1
EXP_NUM ?= 1  # Número do experimento (pode ser alterado na chamada: make all EXP_NUM=2)

# Alvo padrão
all: pandas polars pyspark duckdb

# -------------------
# Experimentos
# -------------------

pandas:
	@echo ">>> Running Pandas experiment..."
	$(DOCKER_RUN) pandas python /app/run_pandas.py \
		--exp $(EXP_NUM) \
		--glob $(DATA_GLOB) \
		--pickup-col $(PICKUP_COL) \
		--dropoff-col $(DROPOFF_COL) \
		--tip-col $(TIP_COL)

polars:
	@echo ">>> Running Polars experiment..."
	$(DOCKER_RUN) polars python /app/run_polars.py \
		--exp $(EXP_NUM) \
		--glob $(DATA_GLOB) \
		--pickup-col $(PICKUP_COL) \
		--dropoff-col $(DROPOFF_COL) \
		--tip-col $(TIP_COL)

pyspark:
	@echo ">>> Running PySpark experiment..."
	$(DOCKER_RUN) pyspark python /app/run_pyspark.py \
		--exp $(EXP_NUM) \
		--glob $(DATA_GLOB) \
		--pickup-col $(PICKUP_COL) \
		--dropoff-col $(DROPOFF_COL) \
		--tip-col $(TIP_COL)

duckdb:
	@echo ">>> Running DuckDB experiment..."
	$(DOCKER_RUN) duckdb python /app/run_duckdb.py \
		--exp $(EXP_NUM) \
		--glob $(DATA_GLOB) \
		--pickup-col $(PICKUP_COL) \
		--dropoff-col $(DROPOFF_COL) \
		--tip-col $(TIP_COL)

# -------------------
# Limpeza
# -------------------
clean:
	@echo ">>> Removing result files..."
	rm -f results/*.parquet results/*.csv
