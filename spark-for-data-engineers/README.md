## Setup 

**Prerequisites**:

1. [docker](https://docs.docker.com/engine/install/) & [docker compose](https://docs.docker.com/compose/)
2. Atleast 4GB (preferably 8GB or more) memory

Start the container by opening a terminal in the downloaded project folder and starting the containers

```bash
docker compose up --build -d --scale spark-worker=2
sleep 30
```

add: WSL 

Open Jupyter lab at **[http://localhost:8888/lab/](http://localhost:8888/lab/)**.

> [!WARNING]
> If you notice issues with [Dockerfile](./Dockerfile#L31) when running `docker compose up`, its most likely due to the fact that Spark version has been bumped. 
> Replace L31 with L29 in the [Dockerfile](./Dockerfile), this will use the archive site to download the Spark jar.
> We don't use the archive link, as it is generally very slow.

> [!CAUTION]
> If your docker is using too much memory, purge them all and restart. This will remove all images and volumes from your local machine

```bash
# Purge all images and volumes to free up space
docker stop $(docker ps -aq)
docker rm -f $(docker ps -aq)
docker rmi -f $(docker images -aq)
docker volume rm $(docker volume ls -q)
```

Stop containers with

```bash
docker compose down -v
sudo rm -rf ./minio_data/*
sudo rm -rf ./notebooks/data/*
sudo rm -rf ./notebooks/notebooks_with_answers/data/*
```

## Licensing & Attribution

### MinIO
This course uses [MinIO](https://min.io) for object storage demonstrations. MinIO is open source software licensed under GNU AGPL v3.

### Stack Overflow Dataset
This course uses the Stack Overflow dataset for examples and exercises.

**Dataset:** Stack Overflow questions and answers  
**Source:** [ClickHouse Example Datasets](https://clickhouse.com/docs/getting-started/example-datasets/stackoverflow)  
**Original Data:** © Stack Exchange Inc.  
**License:** [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/)  

The Stack Overflow data is user-contributed content from the Stack Exchange network, made available under the Creative Commons Attribution-ShareAlike 4.0 International License.

