#!/usr/bin/bash

## CONFIGURACIÓN DE INSTALACIÓN

source parameters.config

if [ $? == 1 ]
then
    echo "Carga incorrecta del archivo de configuración"
else
    if [ $FRESH_INSTALL == 1 ]
    then
        cd $PROJECT_PATH
        mkdir $DATA_PATH $LOG_PATH $JSON_PATH $MARIADB_PATH
        pip install virtualenv
        virtualenv modelodatosabiertos/venv
        source modelodatosabiertos/venv/bin/activate
        pip install -r $PROJECT_PATH/modelodatosabiertos/requirements.txt
        $PROJECT_PATH/modelodatosabiertos/configure_docker.sh
        python $SRC_PATH/main.py
    else
        if [ $S3_DOWNLOAD == 1 ]
        then
            $PROJECT_PATH/modelodatosabiertos/download_s3.sh $S3_BUCKET_NAME $JSON_PATH
        fi
        cd $PROJECT_PATH
        source modelodatosabiertos/venv/bin/activate
        python $SRC_PATH/main.py
    fi
fi
