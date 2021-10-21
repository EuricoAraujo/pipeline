from airflow.decorators import dag,task
from datetime import datetime, timedelta, date
from airflow.models.dag import ScheduleInterval
import pandas as pd
from pymongo import MongoClient
import boto3
from botocore.exceptions import ClientError
import os
from airflow.models import Variable
from airflow.utils.dates import days_ago
from sqlalchemy.sql.elements import Extract
import boto3
from botocore.exceptions import NoCredentialsError


#argumentos default 
default_args = {
    'owner:' : 'eurico',
    'depends_on_past': False,
    #'start_date' : datetime(2021,10,5),
    #'email': ['airflow@airflow.com','igti@igti.com'],
    #'email_on_failure' : False,
    #'retries' : 1,
    #'retry_delay': timedelta(minutes = 5)
}

@dag(description = "estração em API IBGE, tabela mongoDB/ ingestão em AWS ",default_args = default_args,schedule_interval= None, start_date=days_ago(2), tags=['desafiofinal'])
def dag_principal():
    @task
    def extract1():
        #extrai da primeira fonte de dados API do ibge
        captura_da_api = pd.read_json('https://servicodados.ibge.gov.br/api/v1/localidades/distritos')
        #mesoregiao_MG
        #captura_da_api.to_json("/home/eurico/docker-airflow/data/captura_da_api.json")
        caminho_arquivo = "/usr/local/airflow/data/captura_da_api.json"
        captura_da_api.to_json(caminho_arquivo)
        return caminho_arquivo[24:]
    @task
    def extract2(): 
        #extrai da segunda base de dados MongoDB
        CONNECTION_STRING = "mongodb+srv://estudante_igti:SRwkJTDz2nA28ME9@unicluster.ixhvw.mongodb.net/ibge"
        client = MongoClient(CONNECTION_STRING)
        db = client['ibge']
        colecao = db['pnadc20203'].find()
        novodata = pd.DataFrame(colecao)
        novodata.to_json("/usr/local/airflow/data/novodata.json")
    @task
    def transform():
        #considerar apenas mulheres com idade entre 20 e 40 anos
        banco_mongo = pd.read_json('/home/eurico/docker-airflow/data/novodata.json')
        banco_final = banco_mongo.loc[(banco_mongo.idade >= 20) & (banco_mongo.idade <=40) & (banco_mongo.sexo == "Mulher")]
        #banco_final.to_json('/home/eurico/docker-airflow/banco_final.json')
    #@task
    #def load():
    #ir em admin variables no airflow pra configurar as keys
        #carrega arquivos no bucket S3 AWS        
        #aws_acces_key_id = Variable.get('aws_acces_key_id') 
        #aws_secret_acces_key = Variable.get('aws_secret_acces_key')
    @task
    def upload_to_aws(local_file, bucket, s3_file):
        ACCESS_KEY = Variable.get('aws_acces_key_id') 
        SECRET_KEY = Variable.get('aws_secret_acces_key')
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)

        try:
            s3.upload_file(local_file, bucket, s3_file)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False
  #  @task
  ##  def escreve_postgres(csv_file_path):
  #      engine = 
  #      df = pd.read_csv()
   #     df.to_sql()
    nome_bucket = 'igti-bootcamp-ed-2021-360432246556'
    extract1() >> upload_to_aws(extract1(),nome_bucket , extract1())

    #orquestrar

    #api = extract1() 
    #mongo = extract2()
    #upmongo = upload_to_s3()
dag_etl = dag_principal()



