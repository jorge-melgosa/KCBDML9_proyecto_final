"""
Que vamos hacer:

Vamos a procesar el archivo de las suscripciones de los addons

requirements
-----------------------
google-cloud-storage
google-cloud
pandas
"""

import io
from google.cloud import storage
import pandas as pd


def processiong_anfix_addons_subscriptions(event, context):
    FILE = event
    FILE_NAME = FILE['name']
    print(f"Processing file: {FILE_NAME}.")

    STORAGE_CLIENT = storage.Client(project='kc-proyecto-mel-afx')
    BUCKET_RAW = STORAGE_CLIENT.bucket('kc-mel-practica-raw')
    BUCKET_DATA_LAKE = STORAGE_CLIENT.bucket('kc-mel-practica-datalake')

    NAME_FILE_STOCK_RESULT = 'DataSet_anfix_stock.csv'
    NAME_FILE_PROJECTS_RESULT = 'DataSet_anfix_projects.csv'
    NAME_FILE_ADDONS = 'DataSet_anfixdb_addons.csv'

    # si hemos subido el archivo correcto
    if NAME_FILE_ADDONS == FILE_NAME:

        print("- Cargamos el DataFrame con los datos de suscripciones en AnfixDb importados del csv.")
        blob_file_csv = BUCKET_RAW.blob(FILE_NAME)
        data = blob_file_csv.download_as_string()
        df_addons_db = pd.read_csv(io.BytesIO(data))
        print("df:{}".format(df_addons_db))
        print("df.columns:{}".format(df_addons_db.columns))
        print("The number of columns:{}".format(len(df_addons_db.columns)))
        df_addons_sto = pd.DataFrame(columns=df_addons_db.columns)
        df_addons_pro = pd.DataFrame(columns=df_addons_db.columns)

        print('- Nos vamos a quedar con suscripción del addons mas reciente de la compañia en Anfix.')
        companyIds = df_addons_db.companyId.unique().tolist()
        for companyId in companyIds:
            new_row = df_addons_db.loc[df_addons_db['companyId'] == companyId]
            if len(new_row.index) > 0:
                new_sto = new_row.loc[new_row['doc_DirCommercialConfigId'].isin([827, 828, 829, 830, 831, 832])]
                if len(new_sto.index) > 0:
                    df_addons_sto.loc[len(df_addons_sto.index)] = new_sto.iloc[-1]
                new_pro = new_row.loc[new_row['doc_DirCommercialConfigId'].isin([833, 834, 835, 836, 837, 838])]
                if len(new_pro.index) > 0:
                    df_addons_pro.loc[len(df_addons_pro.index)] = new_pro.iloc[-1]

        print("df Stock:{}".format(df_addons_sto))
        print("df.columns:{}".format(df_addons_sto.columns))
        print("The number of columns:{}".format(len(df_addons_sto.columns)))

        print("df Proyectos:{}".format(df_addons_pro))
        print("df.columns:{}".format(df_addons_pro.columns))
        print("The number of columns:{}".format(len(df_addons_pro.columns)))

        df_addons_sto.reset_index(drop=True, inplace=True)
        blob_file_stock_result = BUCKET_DATA_LAKE.blob(NAME_FILE_STOCK_RESULT)
        blob_file_stock_result.upload_from_string(df_addons_sto.to_csv(), 'text/csv')
        print("- Archivo {a} almacenado correctamente en el Data Lake {b}".format(a=NAME_FILE_STOCK_RESULT, b=BUCKET_DATA_LAKE))

        df_addons_pro.reset_index(drop=True, inplace=True)
        blob_file_proyect_result = BUCKET_DATA_LAKE.blob(NAME_FILE_PROJECTS_RESULT)
        blob_file_proyect_result.upload_from_string(df_addons_pro.to_csv(), 'text/csv')
        print("- Archivo {a} almacenado correctamente en el Data Lake {b}".format(a=NAME_FILE_PROJECTS_RESULT, b=BUCKET_DATA_LAKE))

    else:
        print("No hemos subido el archivo correcto.")
