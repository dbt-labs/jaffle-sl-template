# 🥪 The Jaffle Shop 🦘
This repo provides a production example of a dbt project containting [metrics](https://docs.getdbt.com/docs/build/metrics-overview) and [semantic models](https://docs.getdbt.com/docs/build/semantic-models). These resources are required to use the dbt semantic layer. To get started, follow the instructions below: 

## Clone the repo.

```shell
git clone git@github.com:dbt-labs/jaffle-sl-template.git
cd jaffle-sl-template
```

## Install Metricflow

Install metricflow, et al within a virtual environment:
```shell
python -m venv .env
source .env/bin/activate
pip install "dbt-metricflow[<YOUR_DBT_ADAPTER_NAME>]"
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

## Run your dbt project, and query metrics

```shell
dbt build --exclude path:jaffle-data
mf validate-configs
mf query --metrics large_orders
```