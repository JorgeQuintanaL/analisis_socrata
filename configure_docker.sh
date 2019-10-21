#!/usr/bin/bash

docker pull mariadb
docker run --name dev_mariadb -e MYSQL_ROOT_PASSWORD=XXXX -e MYSQL_USER=XXXX -e MYSQL_PASSWORD=XXXX -e MYSQL_DATABASE=XXXX -v XXXX:/var/lib/mysql -d mariadb
docker pull mongo
docker run -d --name dev_mongo -e MONGO_INITDB_ROOT_USERNAME=XXXX -e MONGO_INITDB_ROOT_PASSWORD=XXXX mongo
