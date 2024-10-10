# 🥪 The Jaffle Shop 🦘
This repo provides a production example of a dbt project containting [metrics](https://docs.getdbt.com/docs/build/metrics-overview) and [semantic models](https://docs.getdbt.com/docs/build/semantic-models). These resources are required to use the dbt semantic layer. To get started, follow the instructions below: 

It's been updated to be self-contained for use with duckdb.

## Clone the repo.

```shell
git clone git@github.com:jonmmease/jaffle-sl-template.git
cd jaffle-sl-template
```

# Install pixi
Install pixi

```
pixi install
```

## Check your metricflow version

```
pixi shell
dbt --version
mf --version
```

# Set profile.yaml

Add this to your `~/.dbt/profiles.yml` file:

```yaml
# This setting configures which "profile" dbt uses for this project.
profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '/path/to/jaffle-data/jaffle.db'
      extensions:
        - httpfs
        - parquet
```

update the path to the duckdb file to where you want to store it.

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
dbt build
mf validate-configs
mf query --metrics large_order
```
