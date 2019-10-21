#!/usr/bin/bash

docker pull mariadb
docker run --name dev_mariadb -e MYSQL_ROOT_PASSWORD=root1234 -e MYSQL_USER=quantomm_dev -e MYSQL_PASSWORD=quantomm_dev -e MYSQL_DATABASE=socrata -v /home/jorgequintana/Documents/Quantomm/modelodatosabiertos/data/MariaDB/:/var/lib/mysql -d mariadb
docker pull mongo
docker run -d --name dev_mongo -e MONGO_INITDB_ROOT_USERNAME=quantomm_dev -e MONGO_INITDB_ROOT_PASSWORD=quantomm_dev mongo