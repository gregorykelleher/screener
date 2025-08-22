# Equity Aggregator

## Description

Equity Aggregator is a container-based application designed to provided financial data analysis and visualisation. It leverages various data sources to display information about stocks, sectors, commodities and more.

## Project Structure

The project is organized into several directories. At the root level:

- `data/`: Contains the SQLite database and relevant data
- `docs/`: Contains relevant project documentation
- `packages/`: Contains project packages
- `docker-compose.yml`: Contains the project `Docker` service composition
- `pyproject.toml`: Contains the project metadata, workspace configuration and build system setup

## Setup

The project is managed using the `uv` package manager, and uses `Docker` for containerisation.

### Create Virtual Environment

To create a virtual environment for the project, run:

```sh
uv venv --python 3.12
```

The virtual environment can be activated using:

```sh
source .venv/bin/activate
```

Equally, it's possible to deactivate the virtual environment using:

```sh
source deactivate
```

### Install Packages

Sync the project's dependencies with the environment:

```sh
uv sync
```

> It's possible to install every workspace member as editable using `uv sync --all-packages`

A `uv.lock` lockfile is generated that contains exact information about the project's dependencies.

> Note, that unlike the `pyproject.toml` which is used to specify the broad requirements of the project, the `uv.lock` contains the exact resolved versions that are installed in the project environment.

To install the current project as an editable package:

```sh
uv pip install -e .
```

This command will install all project packages under the `packages` directory (i.e. `equity-aggregator`, `streamlit-app` and `database`).

The project's installed packages can be listed using:

```sh
uv pip list
```

Details on a specific package can be queried:

```sh
uv pip show equity-aggregator
```

And the specific package application can also be executed:

```sh
uv run equity-aggregator
```

Execute with environmental variables:

```sh
uv run --env-file .env equity-aggregator
```

To run a package's tests using its pytest options:

```sh
uv run pytest -c packages/equity-aggregator/pyproject.toml
uv run pytest -vvv -m unit --cov=equity_aggregator --cov-report=term-missing --cov-report=html
uv run pytest -m unit -vvv
```

## Launch the Equity Aggregator Application

The `packages` directory contains the project's packages, managed by `uv`. Each package has a corresponding `pyproject.toml` and is distributable as a Python module.

Those packages that are used as `Docker` services have a `Dockerfile`.

```sh
packages
├── equity-aggregator
│   ├── Dockerfile
│   ├── README.md
│   ├── build
│   ├── pyproject.toml
│   └── src
├── database
│   ├── README.md
│   ├── build
│   ├── pyproject.toml
│   └── src
└── streamlit-app
    ├── Dockerfile
    ├── README.md
    ├── build
    ├── pyproject.toml
    └── src
```

### Shortcut

```sh
# once - setup
uv venv --python 3.12     # makes .venv
uv sync --all-packages    # Builds each packages/* project once, drops a single editable link

# dev loop
uv run equity-aggregator    # run first checks lock -> pyproject -> env, then executes. No need to activate the venv
```

### Docker

The project depends on `Docker v27.4.0`.

Build and launch the containers with:

```sh
docker compose up --build
```

>>> The `--build` flag ensures that `Docker` will rebuild the images for the services prior to starting the containers. Without the flag, `Docker` may use cached versions of the images, potentially missing any recent updates.

Likewise to stop and remove containers:

```sh
docker-compose down
```

To list all active running containers:

```sh
docker ps -a
```

To view the logs from a container:

```sh
docker logs example-container
```

To open a shell to a running container:

```sh
docker-compose exec example-container bash
```

To list available volumes:

```sh
docker volume ls
```

To remove unused data (stopped containers, dangling images and build cache .etc):

```sh
docker system prune
```

To remove all `Docker` containers, active and inactive:

```sh
docker rm -f $(docker ps -aq)
```
#### docker-compose.yml

The `docker-compose.yml` file contains the `Docker` definitions for the project's services (i.e. `streamlit-app` and `equity-aggregator`).

The `streamlit-app` service depends on the `equity-aggregator` service. Both services mount and share the `data` volume (containing the `stocks_universe.db` SQlite database).

To run an individual service:

```sh
docker-compose up streamlit-app
```

To run the `equity-aggregator` service in the background:

```sh
docker-compose up -d
```

## Miscellaneous

### Run the Ruff Formatter and Linter

Run the `ruff` formatter:

```sh
uv run ruff format
```

Run the `ruff` linter:

```sh
uv run ruff check
```

### Query SQLite Database

```sh
sqlite3 data/equities.db
.tables
SELECT * FROM equity_identities;
```
