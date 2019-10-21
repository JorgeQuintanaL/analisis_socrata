from socrata import Socrata_Install
from socrata import S3_Install

from time import time
import logging
import os

if __name__ == "__main__":
    token = os.environ["SOCRATA_TOKEN"]
    limit = os.environ["LIMIT"]
    access_id = os.environ["AWS_ACCESS_KEY_ID"]
    key_id = os.environ["AWS_SECRET_ACCESS_KEY"]
    bucket_name = os.environ["S3_BUCKET_NAME"]
    start = os.environ["START"]
    to_cloud = int(os.environ["TO_CLOUD"])
    to_disk = int(os.environ["SAVE_TO_DISK"])
    json_path = os.environ["JSON_PATH"]
    log_dir = os.environ["LOG_PATH"]
    db_user = os.environ["MARIADB_USER"]
    db_password = os.environ["MARIADB_PASSWORD"]
    db_host = os.environ["MARIADB_HOST"]
    db_database = os.environ["MARIADB_DB"]
    s3_install = int(os.environ["S3_INSTALL"])
    install = int(os.environ["INSTALL"])

    logging.basicConfig(format="%(asctime)s - %(message)s",
                datefmt="%d-%b-%y %H:%M:%S",
                filename="{}/{}.log".format(log_dir, time()), 
                level=logging.DEBUG)
    logging.info("Starting downloading process using the following variables: ")
    logging.info("Token: %s", token)
    logging.info("Limit: %s", limit)
    logging.info("Cloud: %s", to_cloud)
    logging.info("path: %s", json_path)
    logging.info("Install or Update: %s", install)
    logging.info("Source: %s", s3_install)
        
    if install == 1:
        if s3_install == 1:
            print("Installing from S3")
            s3_process = S3_Install(token=token,
                                    limit=limit,
                                    db_user=db_user,
                                    db_password=db_password, 
                                    db_host=db_host, 
                                    db_database=db_database, 
                                    json_path=json_path, 
                                    bucket_name=bucket_name, 
                                    access_id=access_id, 
                                    key_id=key_id)
            s3_process.initial_load()
        else:
            print("Installing from Socrata")
            download_process = Socrata_Install(token=token, 
                                            limit=limit, 
                                            access_id=access_id, 
                                            key_id=key_id,
                                            db_user=db_user,
                                            db_password=db_password,
                                            db_host=db_host,
                                            db_database=db_database,
                                            bucket_name=bucket_name,
                                            start=start, 
                                            to_cloud=to_cloud, 
                                            json_path=json_path,
                                            to_disk=to_disk)
            download_process.download_data()
    elif install == 2:
        if s3_install == 1:
            print("Updating from S3")
            s3_process = S3_Install(token=token,
                                    limit=limit,
                                    db_user=db_user,
                                    db_password=db_password, 
                                    db_host=db_host, 
                                    db_database=db_database, 
                                    json_path=json_path, 
                                    bucket_name=bucket_name, 
                                    access_id=access_id, 
                                    key_id=key_id)
            s3_process.update()
        else:
            print("Updating from Socrata")
            download_process = Socrata_Install(token=token, 
                                            limit=limit, 
                                            access_id=access_id, 
                                            key_id=key_id,
                                            db_user=db_user,
                                            db_password=db_password,
                                            db_host=db_host,
                                            db_database=db_database,
                                            bucket_name=bucket_name,
                                            start=start, 
                                            to_cloud=to_cloud, 
                                            json_path=json_path)
            download_process.update()
    else:
        if s3_install == 1:
            print("Installing and Updating from S3")
            s3_process = S3_Install(token=token,
                                    limit=limit,
                                    db_user=db_user,
                                    db_password=db_password, 
                                    db_host=db_host, 
                                    db_database=db_database, 
                                    json_path=json_path, 
                                    bucket_name=bucket_name, 
                                    access_id=access_id, 
                                    key_id=key_id)
            s3_process.initial_load()
            s3_process.update()
        else:
            print("Installing and Updating from Socrata")
            download_process = Socrata_Install(token=token, 
                                            limit=limit, 
                                            access_id=access_id, 
                                            key_id=key_id,
                                            db_user=db_user,
                                            db_password=db_password,
                                            db_host=db_host,
                                            db_database=db_database,
                                            bucket_name=bucket_name,
                                            start=start, 
                                            to_cloud=to_cloud, 
                                            json_path=json_path)
            download_process.download_data()
            download_process.update()



