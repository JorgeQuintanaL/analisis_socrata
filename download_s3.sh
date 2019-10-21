#!/usr/bin/bash

########################################################################################################
########################################################################################################
###                                                                                                #####
### ESTE CÓDIGO GENERA LA DESCARGA DE LOS ARCHIVOS EN FORMATO JSON DESDE EL REPOSITORIO QUE EXISTE #####
### EN S3. lOS ARCHIVOS SE DESCARGAN A UN DIRECTORIO DEFINIDO POR EL USUARIO PARA LUEGO GENERAR LA ##### 
### CARGA A MARIADB DE LA INFORMACIÓN DE LOS DATASETS                                              #####
###                                                                                                #####
### LAS VARIABLES QUE RECIBE PARA EJECUTARSE SON LAS SIGUIENTES:                                   #####
### S3_BUCKET: NOMBRE DEL REPOSITORIO EN S3 DE DONDE SE VAN A DESCARGAR LOS ARCHIVOS               #####
### LOCAL_DIR: RUTA DE LA CARPETA LOCAL EN DONDE SE VAN A GUARDAR LOS ARCHIVOS JSON                #####
###                                                                                                #####
########################################################################################################
########################################################################################################

S3_BUCKET=$1
LOCAL_DIR=$2

function synchronization()
{
    echo "Starting synchronization process using the following parameters"
    echo "S3_BUCKET: " $S3_BUCKET " and LOCAL_DIR: " $LOCAL_DIR
    aws s3 sync s3://$S3_BUCKET $LOCAL_DIR --region us-east-1
    if [ $? == 0 ]
    then
        return 0
    else
        return 1
    fi
}

if [ $# -ne 2 ]
then
	echo "Illegal number of parameters"
	exit 1
else
    echo "Checking for AWSCLI installed on this machine"
    aws --version
    if [ $? == 0 ]
    then
        synchronization
    else
        echo "AWSCLI not detected. Installing using pip..."
        pip install awscli
        if [ $? == 0 ]
        then
            echo "Installation process successful!"
            synchronization
        else
            echo "Installation process failed. Try to install AWSCLI manually!"
        fi
    fi
fi
