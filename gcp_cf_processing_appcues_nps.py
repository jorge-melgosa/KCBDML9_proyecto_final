"""
Que vamos hacer:
Vamos a procesar el archivo donde tenemos almacenados los nps

Atributos que vamos almacenar
- timestamp
- attributes.score
- attributes.feedback
- attributes._prev_nps_score
- attributes._identity.companyCorporateName
- attributes._identity.companyId
- attributes._identity.organizationId

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

    NAME_FILE_RESULT = 'DataSet_appcues_nps.csv'
    NAME_FILE_NPS = 'DataSet_appcues_nps.csv'

    columns_nps_raw = [
        'timestamp',
        'attributes.score',
        'attributes.feedback',
        'attributes._prev_nps_score',
        'attributes._identity.companyCorporateName',
        'attributes._identity.companyId',
        'attributes._identity.organizationId'
    ]

    columns_nps = [
        'timestamp',
        'score',
        'feedback',
        'prev_nps_score',
        'companyCorporateName',
        'companyId',
        'organizationId'
    ]

    # si hemos subido el archivo correcto
    if NAME_FILE_NPS == FILE_NAME:

        print("- Cargamos el DataFrame con los datos del nps que Appcues recoge.")
        blob_file_csv = BUCKET_RAW.blob(FILE_NAME)
        data = blob_file_csv.download_as_string()
        df_nps_raw = pd.read_csv(io.BytesIO(data))
        print("df:{}".format(df_nps_raw))
        print("df.columns:{}".format(df_nps_raw.columns))
        print("The number of columns:{}".format(len(df_nps_raw.columns)))
        df_nps = pd.DataFrame(columns=columns_nps)

        print('- Nos vamos a quedar con las columnas que nos interesan del df original')
        df_nps_raw = df_nps_raw[columns_nps_raw]
        print("df:{}".format(df_nps_raw))
        print("The number of columns:{}".format(len(df_nps_raw.columns)))

        companyIds = df_nps_raw['attributes._identity.companyId'].unique().tolist()
        for companyId in companyIds:
            # nos vamos a quedar con la última entrada para el nps
            df_new = df_nps_raw.loc[df_nps_raw['attributes._identity.companyId'] == companyId].iloc[-1]
            df_new = df_new.fillna('')

            new_row = {
                'timestamp': df_new['timestamp'],
                'score': df_new['attributes.score'],
                'feedback': df_new['attributes.feedback'],
                'prev_nps_score': df_new['attributes._prev_nps_score'],
                'companyCorporateName': df_new['attributes._identity.companyCorporateName'],
                'companyId': df_new['attributes._identity.companyId'],
                'organizationId': df_new['attributes._identity.organizationId']
            }

            df_nps.loc[len(df_nps.index)] = new_row

        df_nps = df_nps[columns_nps]
        print("df:{}".format(df_nps))
        print("df.columns:{}".format(df_nps.columns))
        print("The number of columns:{}".format(len(df_nps.columns)))

        df_nps.reset_index(drop=True, inplace=True)
        blob_file_stock_result = BUCKET_DATA_LAKE.blob(NAME_FILE_RESULT)
        blob_file_stock_result.upload_from_string(df_nps.to_csv(), 'text/csv')
        print(
            "- Archivo {a} almacenado correctamente en el Data Lake {b}".format(a=NAME_FILE_RESULT, b=BUCKET_DATA_LAKE))

    else:
        print("No hemos subido el archivo correcto.")
