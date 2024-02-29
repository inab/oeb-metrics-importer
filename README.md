# OPEB Metrics importer 

Imports those entries in OPEB metrics that were originally retrieved from bioconda, biotools and galaxy.eu.

## Set-up and Usage

### Option 1 (RECOMENDED) - Docker container 
The easiest way to run this importer is by using a docker image.
1. Pull the image 

    ```sh
    docker login registry.gitlab.bsc.es
    docker pull registry.gitlab.bsc.es/inb/elixir/software-observatory/opeb-metrics-importer
    ```

2. Run the container. 
If the ENV variables are stored in an `.env` file: 
    ```sh
    docker run --name [container-name] --env-file .env registry.gitlab.bsc.es/inb/elixir/software-observatory/opeb-metrics-importer
    ```

> :bulb: **Using `linux/amd64` architecture to run (and build) the container** 
>
>```sh
>export DOCKER_DEFAULT_PLATFORM=linux/amd64 
>```
> Necessary to run this container in a MacBook with M1 chip.


> :bulb: **Connecting to services in host** 
>
> Use `host.docker.internal` instead of `localhost` in the container to reach local services. For instance, to connect to a local MongoDB, use the string `host.docker.internal:27017`. 


### Option 2 - Native 

1. Clone this repository.

2. Install Python packages listed in `requirements.txt`.

    ```sh
    pip install -r requirements.txt
    ```

3. Execute the importer

    ```sh
    python3 main.py -l=[log-level] -d=[log-directory]
    ```
    - `-l`/`--loglevel` is optional. It can be `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL`. Default is `INFO`.
    - `-d`/`--logdir` is optional. It specifies the directory where the log file will be stored. Default is `./logs/`.

> This program has been successfully executed using Python 3.8 and 3.9.


## Configuration

### Environment variables 

| Name             | Description | Default | Notes |
|------------------|-------------|---------|-------|
| HOST       |  Host of database where output will be pushed |   `localhost`        |  |
| PORT       |  Port of database where output will be pushed |   `27017`            |  |
| USER       |  User of database where output will be pushed |            |  |
| PASS   |  Password of database where output will be pushed |            |  |
| AUTH_SRC  |  Authentication source of database where output will be pushed |   `admin`  |  |
| DB         |  Name of database where output will be pushed |   `observatory`      |  |
| ALAMBIQUE |  Name of database where output will be pushed  |   `alambique`        |  |
| WEBMETRICS | Name of database where output will be pushed  |   `webMetrics`       |  |
| PUBLICATIONS | Name of database where output will be pushed  |   `publications`    |  |
| URL_OPEB_METRICS | URL to OpenEBench Metrics API | `https://openebench.bsc.es/monitor/metrics/` | |
## CI/CD

This repository is integrated with GitLab CI/CD. The pipeline is defined in `.gitlab-ci.yml`. It is composed of the following stages:

| Stage | Description | Runs |
|-------|-------------|------|
| `prepare` | Pulls bioconda recipes. It is necessary for both testing and data importation | Always |
| `test` | Runs the tests | Always |
| `main_task` | Data importation | Manually or on schedule |


