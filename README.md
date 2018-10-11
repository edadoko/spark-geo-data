## Environment

Unless you already have a working [Apache Spark](http://spark.apache.org/) cluster, you will need to have [Docker](https://docs.docker.com/) for simple environment setup.

The provided `docker-compose.yml` and Spark configurations in `conf` directory are cloned from <https://github.com/gettyimages/docker-spark>.

## Setup

0. Make sure Docker is installed properly and `docker-compose` is ready to use
1. Run `$ docker-compose up -d` under the `data-mr` directory
2. Check Spark UI at `http://localhost:8080` and you should see 1 master and 1 worker
3. Run `$ docker exec -it datamr_master_1 /bin/bash` to get into the container shell, and start utilizing Spark commands such as `# spark-shell`, `# pyspark` or `# spark-submit`. You may want to replace `datamr_master_1` with actual container name that's spawned by the `docker-compose` process

![demo.gif](https://user-images.githubusercontent.com/2837532/27649289-4fdffd52-5bff-11e7-9236-0a1d063461cb.gif)

## Build Instructions

Run spark jobs using spark-submit: `spark-submit /tmp/src/script.py` 

