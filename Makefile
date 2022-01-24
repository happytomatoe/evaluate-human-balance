# Adapted from https://github.com/9gl/python/blob/2d8f03367f7b430738f25b3d1aa891c3df1cf069/py_automation/Makefile

.PHONY: help prepare-dev test lint 	doc default

VENV_NAME?=venv
VENV_ACTIVATE=. $(VENV_NAME)/bin/activate
PYTHON_VENV=${VENV_NAME}/bin/python3
PYSPARK_VENV=${VENV_NAME}/bin/pyspark
PYTHON_LOCAL=python3

default: create-venv run

.DEFAULT: help
help:
	@echo "make prepare-dev"
	@echo "       prepare development environment, use only once"
	@echo "make test"
	@echo "       	tests"
	@echo "make lint"
	@echo "       	pylint and mypy"
	@echo "make run"
	@echo "       	project"
	@echo "make doc"
	@echo "       build sphinx documentation"

prepare-dev:
	sudo apt-get -y install python3.8 python3-pip

create-venv:
	python3 -m pip install virtualenv

venv: requirements.txt
	test -d $(VENV_NAME) || virtualenv -p python3 $(VENV_NAME)
	${PYTHON_VENV} -m pip install -U pip
	${PYTHON_VENV} -m pip  install  -r requirements.txt
	touch $(VENV_NAME)/bin/activate


test: venv
	${PYTHON_VENV} -m pytest ./airflow/dags/scripts

lint: venv
	${PYTHON_VENV} -m pylint src/
	${PYTHON_VENV} -m mypy

jupyter-install: venv
	( \
	$(VENV_ACTIVATE); \
	jupyter contrib nbextension install --user; \
	jupyter nbextension enable snippets_menu/main; \
	jupyter nbextension enable zenmode/main; \
	jupyter nbextension enable contrib_nbextensions_help_item/main; \
	jupyter nbextension enable autosavetime/main; \
	jupyter nbextension enable codefolding/main; \
	jupyter nbextension enable code_font_size/code_font_size; \
	jupyter nbextension enable code_prettify/code_prettify; \
	jupyter nbextension enable collapsible_headings/main; \
	jupyter nbextension enable comment-uncomment/main; \
	jupyter nbextension enable equation-numbering/main; \
	jupyter nbextension enable execute_time/ExecuteTime; \
	jupyter nbextension enable gist_it/main; \
	jupyter nbextension enable hide_input/main; \
	jupyter nbextension enable spellchecker/main; \
	jupyter nbextension enable toc2/main; \
	jupyter nbextension enable toggle_all_line_numbers/main; \
	\
	jupyter nbextension enable code_prettify/isort; \
	jupyter nbextension enable select_keymap/main; \
	jupyter nbextension enable table_beautifier/main; \
	jupyter nbextension enable move_selected_cells/main; \
	jupyter nbextension enable scratchpad/main; \
	jupyter nbextension enable skip-traceback/main; \
	jupyter nbextension enable tree-filter/index; \
	jupyter nbextension enable varInspector/main; \
	jupyter nbextension enable snippets/main; \
	jupyter nbextension enable scroll_down/main; \
	\
	jupyter nbextension enable navigation-hotkeys/main; \
    jupyter nbextension enable code_prettify/autopep8; \
    \
    jupyter nbextension enable runtools/main; \
    #	jupyter nbextension enable keyboard_shortcut_editor/main \
    # disable help(H) \
    jupyter nbextension enable livemdpreview/livemdpreview; \
    jupyter nbextension enable help_panel/help_panel; \
    jupyter nbextension enable autoscroll/main; \
    jupyter nbextension enable limit_output/main; \
)

run: jupyter-install

	$(VENV_ACTIVATE)&&export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2  pyspark-shell"&& jupyter notebook scripts/spark_py_redis_kafka_stream_to_console.ipynb

doc: venv
	$(VENV_ACTIVATE) && cd docs; make html

clean:
	rm -rf venv
	find -iname "*.pyc" -delete