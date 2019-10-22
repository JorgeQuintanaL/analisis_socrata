### Archivo para descargar los dataset desde la página de [Datos Abiertos](http://www.datos.gov.co)

Para replicar el ambiente en el cual se ha desarrollado el código se debe:

* Generar un ambiente mediante virtualenv ( virtualenv venv )
* Se deben instalar todas las dependencias ( pip install -r requirements.txt )
* Se debe activar el ambiente antes creado ( source venv/bin/activate )

Para empezar el proceso se debe ejecutar el archivo run_download.sh, el cual exporta algunas variables de entorno necesarias para ejecutar el proceso. De igual forma, el proceso genera un log que permite hacer seguimiento a cada parte del código. Finalmente, los datasets de guardan en la carpeta data.

Para revisar el avance, tanto en los resultados, como en el diseño de las visualizaciones se puede revisar http://18.212.123.141/

## NO CAMBIAR LA ESTRUCTURA DEL PROYECTO NI EL CÓDIGO SIN PREVIO ACUERDO, YA QUE PROBLEMAS POR FUERA DEL AMBIENTE DE DESARROLLO NO PODRÁN SER RESUELTOS.
