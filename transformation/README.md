dbt init duck_quant


### Configure the profiles.yml file
Set up the `profiles.yml` file to define the connection with the [adapter](https://docs.getdbt.com/docs/trusted-adapters) destination.

## Load environment variables
```sh
export $(cat .env | xargs)
```

## Useful Commands
### Verify dbt configuration
```sh
dbt debug
```
This command validates that the dbt configuration (connection, profiles, and environment) is working correctly.

## Run dbt models

### Important: Specify the `country` parameter

The country parameter is required to dynamically define schemas and paths specific to each country.

Always include this parameter in your dbt executions to ensure the models run for the desired country.

To pass the country parameter, use the `--vars` flag like this:
```sh
dbt run
```
Replace **`col`** with the country code you want to process (e.g., **`arg`** for Argentina).

### Run all models:
```sh
dbt run
```

### Run a specific model:
To execute a specific `.sql` model, provide the model name (**without** the `.sql` extension) and the country parameter:
```sh
dbt run --select model_name
```

### Run models with a specific tag (e.g., tracking)
```sh
dbt run --select tag:tracking
```

### Run models at a specific level (e.g., bronze) with a specific profile and target environment
```sh
dbt run --select bronze --profile mailamericas --target prod
```

### Materialize seeds
```sh
dbt seed
```
