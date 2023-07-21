# 🥪 The Jaffle Shop 🦘

## Clone

```shell
git clone git@github.com:dbt-labs/jaffle-sl-template.git
cd jaffle-sl-template
```

## Install

Install metricflow, et al within a virtual environment:
```shell
python -m venv .env
source .env/bin/activate
pip install "dbt-metricflow[<YOUR_DBT_ADAPTER_NAME>]"
source .env/bin/activate
dbt --version
mf --version
```

## Test the connection
1. Update the `profile` within `dbt_project.yml` to refer to one of your pre-existing profile

```shell
dbt debug
```

## Load data

```shell
dbt deps
dbt seed
```

## Run

```shell
dbt build --exclude path:jaffle-data
mf validate-configs
mf query --metrics large_orders
```