#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = nc_pipeline
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

################################################################################################################
# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install flake8
flake:
	$(call execute_in_env, $(PIP) install flake8)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Set up dev requirements (bandit, safety, flake8)
dev-setup: bandit safety flake coverage


# Build / Run

## Run the security test (bandit + safety)
security-test:
	$(call execute_in_env, safety check -r ./requirements.txt)
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

## Run the flake8 code check
run-flake:
	$(call execute_in_env, flake8  ./src/*/*.py ./tests/*/*.py)

## Run the unit tests
unit-test1:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v tests/test_extraction/)

unit-test2:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v tests/test_processing/)

unit-test3:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v tests/test_storage/)

## Run the coverage check
check-coverage1:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run --omit 'venv/*' -m pytest tests/test_extraction/ && coverage report -m)

check-coverage2:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run --omit 'venv/*' -m pytest tests/test_processing/ && coverage report -m)

check-coverage3:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run --omit 'venv/*' -m pytest tests/test_storage/ && coverage report -m)


# ## Run all checks
run-checks: security-test run-flake unit-test1 unit-test2 unit-test3 check-coverage1 check-coverage2 check-coverage3
