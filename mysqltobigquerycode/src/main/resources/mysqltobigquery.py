import apache_beam as beam
import json
import pandas as pd
import mysql.connector
import os
from google.cloud import bigquery
import pytz
from datetime import datetime
from google.cloud.client import Client

from google.cloud.bigquery import schema, table
from google.cloud.client import Client




class SplitRow(beam.DoFn):
    def __init__(self):
        print("main")
    def process(self,element):
        #self.element=element
        self.element = element
        self.path_data = open(self.element)
        self.data = json.load(self.path_data)
        self.host=self.data["host_name"]
        self.user = self.data["user_name"]
        self.pwd = self.data["user_password"]
        self.DB = self.data["db_name"]
        self.q1=self.data["q1"]

        def connect_to_sql():
            try:
                mydb = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.pwd,
                    database=self.DB
                )

                mycursor = mydb.cursor()
                mycursor.execute(self.q1)

                myresult = mycursor.fetchall()
                df = pd.DataFrame(myresult, columns=["ID", "moviename", "actor", "releasedate"])



                return  df
            except os.error as e:
                print(f"error while {e}")

        self.sql_data=connect_to_sql()

        yield self.sql_data




class FilterAccountsEmployee(beam.DoFn):
    def __init__(self,inputkey):
        self.key=inputkey
        print(self.key)
    def start_bundle(self):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.key
        self.client = bigquery.Client()
        print(self.client)
        self.table_id = "mapapi-290312.sql_data.movies"
            #bigquery.Client()

    def process(self, element):
        self.table_data=element
        self.dataframe=pd.DataFrame(self.table_data)
        ist = pytz.timezone('Asia/Calcutta')

        print('Current Date and Time in IST =', datetime.now(ist))
        timenow = datetime.now(ist)
        self.dataframe["bqload_time"] = timenow
        print(self.dataframe)

        #"ID", "moviename", "actor", "releasedate"

        schema = [bigquery.SchemaField("ID", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("moviename", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("actor", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("releasedate", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("bqload_time", "STRING", mode="NULLABLE")]
        job_config = bigquery.LoadJobConfig(schema=schema)
        df = self.dataframe.astype(str)
        self.client.load_table_from_dataframe(df, self.table_id, job_config=job_config).result()



p1 = beam.Pipeline()
inputpath = ["C:/Users/surya/PycharmProjects/AMS/processgate1/connections.json"] ## path from processgate file json
keypath="E:\gcp\key.json" # bigquery key
attendance_count = (
        p1
        | beam.Create(inputpath)
        | beam.ParDo(SplitRow())
        | beam.ParDo(FilterAccountsEmployee(keypath))
)

p1.run()