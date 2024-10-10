# Install pixi
Install pixi

```
pixi install
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