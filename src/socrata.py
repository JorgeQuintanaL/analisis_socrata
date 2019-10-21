from unidecode import unidecode as decode
from db import MariaDB_Connect
import boto3
from tqdm import tqdm
from datetime import datetime
import requests
import pandas as pd
import numpy as np
import json
import os

class Socrata:
    def __init__(self, token, limit, bucket_name, db_user, db_password, db_host, db_database, access_id, key_id, to_disk=0, start=0, to_cloud=0, json_path="data/JSON/"):
        self.token = token
        self.start = int(start)
        self.limit = int(limit)
        self.to_cloud = to_cloud
        self.to_disk = to_disk
        self.json_path = json_path
        self.bucket_name = bucket_name
        self.access_id = access_id
        self.key_id = key_id
        self.user = db_user
        self.password = db_password
        self.host = db_host
        self.database = db_database
        self.metadata = []
        self.contador = 0

    def read_metadata(self):
        scroll_id = ""
        self.contador = 0
        index = 0
        while True:
            if index == 0:
                resp = requests.get("https://api.us.socrata.com/api/catalog/v1?domains=datos.gov.co&scroll_id={}&limit={}".format(scroll_id, self.limit))
                if resp.status_code == 200:
                    self.metadata = pd.DataFrame.from_dict(resp.json()["results"])
                    if not self.metadata.empty:
                        self.metadata = self.metadata.iloc[[item["type"] == "dataset" for item in self.metadata["resource"]]]
                        self.metadata["dataset_link"] = [item.split("www.datos.gov.co/d/")[-1] for item in self.metadata["permalink"]]
                        self.metadata["dataset_name"] = [dict(item)["name"] for item in self.metadata["resource"]]
                        category = []
                        for item in self.metadata["classification"]:
                            try:
                                cat = dict(item)["domain_category"]
                            except KeyError:
                                cat = None
                            category.append(cat)
                        self.metadata["category"] = category
                        scroll_id = self.metadata.iloc[-1, :][6]["id"]
                        self.contador += self.metadata.shape[0]
                        index += 1
                        print("Total Instances Fetched: {}".format(self.contador))
                    else:
                        break
                else:
                    break
            else:
                resp = requests.get("https://api.us.socrata.com/api/catalog/v1?domains=datos.gov.co&scroll_id={}&limit={}".format(scroll_id, self.limit))
                if resp.status_code == 200:
                    aux = pd.DataFrame(resp.json()["results"])
                    if not aux.empty:
                        aux = aux.iloc[[item["type"] == "dataset" for item in aux["resource"]]]
                        aux["dataset_link"] = [item.split("www.datos.gov.co/d/")[-1] for item in aux["permalink"]]
                        aux["dataset_name"] = [dict(item)["name"] for item in aux["resource"]]
                        category = []
                        for item in aux["classification"]:
                            try:
                                cat = dict(item)["domain_category"]
                            except KeyError:
                                cat = None
                            category.append(cat)
                        aux["category"] = category
                        scroll_id = aux.iloc[-1, :][6]["id"]
                        self.contador += aux.shape[0]
                        self.metadata = pd.concat([self.metadata, aux], axis=0)
                        index += 1
                        print("Total Instances Fetched: {}".format(self.contador))
                    else:
                        break
                else:
                    break
        print("Returning {} records".format(self.metadata.shape[0]))

    def download_dataset(self, item, updated=0, new=0, download=0):
        values = None
        complete_data = []
        if download == 1:
            data_final = None
            index = 0
            offset = 0
            records = requests.get("https://www.datos.gov.co/resource/{}.json?$select=count(*)".format(item["dataset_link"])).json()[0]
            keys = records.keys()
            records = int([records[key] for key in keys][0])
            iterations = np.ceil(records / self.limit)
            if iterations > 0:
                while index < iterations:
                    if index == 0:
                        url = "https://www.datos.gov.co/resource/{}.json?$$app_token={}&$limit={}&$offset={}".format(item["dataset_link"], self.token, self.limit, offset)
                        resp = requests.get(url)
                        if resp.status_code == 200:
                            data_final = pd.DataFrame(json.loads(resp.content, encoding="UTF-8"))
                            if not data_final.empty:
                                offset = (self.limit * index) + 1
                                index += 1
                            else:
                                index = iterations + 1
                        else:
                            print("Bad Request!")
                            index = iterations + 1
                    else:
                        url = "https://www.datos.gov.co/resource/{}.json?$$app_token={}&$limit={}&$offset={}".format(item["dataset_link"], self.token, self.limit, offset)
                        resp = requests.get(url)
                        if resp.status_code == 200:
                            aux = pd.DataFrame(json.loads(resp.content, encoding="UTF-8"))
                            if not aux.empty:
                                data_final = pd.concat([data_final, aux], axis=0, sort=True)
                                offset = (self.limit * index) + 1
                                index += 1
                            else:
                                index = iterations + 1
                        else:
                            print("Bad Request!")
                            index = iterations + 1

                complete_data.append({"metadata": item["metadata"],
                                    "resource": item["resource"],
                                    "dataset": item["dataset_name"],
                                    "data": data_final.to_dict()})

        values = {"id": item["resource"]["id"],
                "nombre": decode(item["resource"]["name"].replace(",", "")) if item["resource"]["name"] else "",
                "categoria": decode(item["category"]) if item["category"] else "",
                "entidad": decode(item["resource"]["attribution"].replace(",", "")) if item["resource"]["attribution"] else "",
                "descripcion": decode(item["resource"]["description"].replace(",", "")) if item["resource"]["description"] else "",
                "fecha_ejecucion": datetime.today().strftime("%Y-%m-%d"),
                "fecha_creacion": item["resource"]["createdAt"][0:10] if item["resource"]["createdAt"] else "",
                "fecha_actualizacion": item["resource"]["updatedAt"][0:10] if item["resource"]["updatedAt"] else "",
                "fecha_datos_actualizados": item["resource"]["data_updated_at"][0:10] if item["resource"]["data_updated_at"] else "",
                "fecha_metadata_actualizada": item["resource"]["metadata_updated_at"][0:10] if item["resource"]["metadata_updated_at"] else "",
                "actualizado": updated,
                "nuevo": new}
        return complete_data, values

    def update(self):
        self.read_metadata()
        rows, _ = self.metadata.shape
        for i in tqdm(range(rows)):
            item = self.metadata.iloc[i, :]
            id_ = item["resource"]["id"]
            new_date = item["resource"]["data_updated_at"][0:10]
            db_connection = MariaDB_Connect(user=self.user,
                                        password=self.password,
                                        host=self.host,
                                        database=self.database)
            db_connection.connect_db()
            old_date = db_connection.search_by_id(id_)
            db_connection.close_db()
            del db_connection
            if old_date:
                if pd.to_datetime(old_date["updated_date"]) < pd.to_datetime(new_date):
                    complete_data, values = self.download_dataset(item, updated=1)
                    db_connection = MariaDB_Connect(user=self.user,
                                        password=self.password,
                                        host=self.host,
                                        database=self.database)
                    db_connection.connect_db()
                    db_connection.update_dataset(values["id"], values["updated_date"], values["updated"])
                    db_connection.close_db()
                    del db_connection
                    self.save_to_disk(item, complete_data)
                    if self.to_cloud == 1:
                        self.save_to_cloud(item)
            else:
                complete_data, values = self.download_dataset(item, new=1)
                db_connection = MariaDB_Connect(user=self.user,
                                        password=self.password,
                                        host=self.host,
                                        database=self.database)
                db_connection.connect_db()
                db_connection.insert_dataset(values)
                db_connection.close_db()
                del db_connection
                self.save_to_disk(item, complete_data)
                if self.to_cloud == 1:
                    self.save_to_cloud(item)

    def save_to_disk(self, item, complete_data):
        with open("{}/{}.json".format(self.json_path, item["resource"]["id"]), "w", encoding="utf-8") as json_file:
                json_file.write(json.dumps(complete_data, indent=4, separators=(",", ":"), ensure_ascii=False))

    def save_to_cloud(self, item):
        Object_ = boto3.resource("s3", aws_access_key_id=self.access_id, aws_secret_access_key=self.key_id).Object(bucket_name=self.bucket_name, key="{}.json".format(item["resource"]["id"]))
        Object_.upload_file("{}/{}.json".format(self.json_path, item["resource"]["id"]))


class S3_Install(Socrata):
    def __init__(self, token, limit, db_user, db_password, db_host, db_database, json_path, bucket_name, access_id, key_id):
        super().__init__(token=token, limit=limit, db_user=db_user, db_password=db_password, db_host=db_host, db_database=db_database, json_path=json_path, bucket_name=bucket_name, access_id=access_id, key_id=key_id)

    def initial_load(self):
        db_connection = MariaDB_Connect(user=self.user,
                                        password=self.password,
                                        host=self.host,
                                        database=self.database)
        db_connection.connect_db()
        db_connection.init_db()
        db_connection.close_db()
        files = os.listdir(self.json_path)
        for file in tqdm(files):
            with open(self.json_path + "/{}".format(file), encoding="utf-8") as json_file:
                aux = json.load(json_file)
            values = {"id": aux[0]["resource"]["id"],
                    "name": decode(aux[0]["resource"]["name"].replace(",", "")) if aux[0]["resource"]["name"] else "",
                    "attribution": decode(aux[0]["resource"]["attribution"].replace(",", "")) if aux[0]["resource"]["attribution"] else "",
                    "description": decode(aux[0]["resource"]["description"].replace(",", "")) if aux[0]["resource"]["description"] else "",
                    "created_date": aux[0]["resource"]["createdAt"][0:10] if aux[0]["resource"]["createdAt"] else "",
                    "updated_date": aux[0]["resource"]["updatedAt"][0:10] if aux[0]["resource"]["updatedAt"] else "",
                    "data_updated": aux[0]["resource"]["data_updated_at"][0:10] if aux[0]["resource"]["data_updated_at"] else "",
                    "metadata_updated": aux[0]["resource"]["metadata_updated_at"][0:10] if aux[0]["resource"]["metadata_updated_at"] else "",
                    "updated": 0, 
                    "new": 0}
            db_connection = MariaDB_Connect(user=self.user,
                                        password=self.password,
                                        host=self.host,
                                        database=self.database)
            db_connection.connect_db()
            db_connection.insert_dataset(values)
            db_connection.close_db()

class Socrata_Install(Socrata):
    def __init__(self, token, limit, access_id, key_id, db_user, db_password, db_host, db_database, bucket_name, to_disk, start=0, to_cloud=1, json_path="data/JSON/"):
        super().__init__(token=token, 
                        limit=limit, 
                        access_id=access_id, 
                        key_id=key_id, 
                        start=start, 
                        to_cloud=to_cloud,
                        to_disk=to_disk,
                        json_path=json_path, 
                        db_user=db_user, 
                        db_password=db_password, 
                        db_host=db_host, 
                        db_database=db_database, 
                        bucket_name=bucket_name)

    def download_data(self):
        db_connection = MariaDB_Connect(user=self.user,
                                        password=self.password,
                                        host=self.host,
                                        database=self.database)
        db_connection.connect_db()
        db_connection.init_db()
        db_connection.close_db()
        self.read_metadata()
        rows, _ = self.metadata.shape
        for i in tqdm(range(self.start, rows)):
            item = self.metadata.iloc[i, :]
            complete_data, values = self.download_dataset(item, download=0)
            if values:
                db_connection = MariaDB_Connect(user=self.user,
                                            password=self.password,
                                            host=self.host,
                                            database=self.database)
                db_connection.connect_db()
                if db_connection.search_by_id(values["id"]) is None:
                    db_connection.insert_dataset(values)
                db_connection.close_db()
                if self.to_disk == 1:
                    self.save_to_disk(item, complete_data)
                if self.to_cloud == 1:
                    self.save_to_cloud(item)
