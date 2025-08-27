Ingestion worker - ingests data from external API.

To run ingestion locally, first build the `docker compose` stack:
```bash
$ docker compose build
```

Then, perform a local test run for a worker, writing ingested data to a local file:
```bash
$ docker compose up broker cache --wait
$ docker compose run --rm ingest-worker --resource-name="311-cases" --max-pages=1 --write-to-file --skip-wait
```

and check the run's output:
```bash
$ redis-cli -u redis://localhost:8379/0 get "ingest-checkpoint:311-cases"
$ wc -l data/311-cases/*.avro
```

To perform a local test run and write to Kafka rather than a file, omit the
`--write-to-file` flag:
```bash
$ docker compose run --rm worker --resource-name="311-cases" --max-pages=1 --skip-wait
```

To run tests, outside the `docker compose` stack, first install the project
using `uv`:
```bash
$ uv sync
```

then, run tests:
```bash
$ uv run python -m pytest tests/
```
