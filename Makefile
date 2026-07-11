SHELL := /usr/bin/env bash
RUN_ID ?= run-20260711T030500Z-60013511
CLI := ./bin/amazon-rec --run-id $(RUN_ID)
PYTHON ?= $(shell PYENV_VERSION=bil401_env_1 pyenv which python 2>/dev/null)
JAVA_HOME := /usr/lib/jvm/java-21-openjdk-amd64
TEST_JUNIT ?= artifacts/test-results/junit.xml
TEST_SHARD_DIR ?= artifacts/test-results/shards

.PHONY: bootstrap test status smoke etl train evaluate dashboard performance all g0 g1 g2 g3 g4 g5 g6 g7 g8 g9 g10 g11 g12

bootstrap:
	JAVA_HOME=$(JAVA_HOME) $(PYTHON) -m pip install -r requirements.lock -e .

test:
	JAVA_HOME=$(JAVA_HOME) SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=src \
		$(PYTHON) scripts/run_tests_sharded.py \
		--output $(TEST_JUNIT) --shard-dir $(TEST_SHARD_DIR)

status:
	$(CLI) status

smoke etl train evaluate dashboard performance all:
	$(CLI) $@

g0:
	@echo "Use scripts/g0_smoke.py through the documented spark-submit command; existing run evidence is preserved."

g1 g2 g3 g4 g5 g6 g7 g8 g9 g10 g11 g12:
	$(CLI) gate G$(patsubst g%,%,$@)
