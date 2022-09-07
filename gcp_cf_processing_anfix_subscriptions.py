"""
Que vamos hacer:

Vamos a procesar el archivo de las suscripciones

requirements
-----------------------
google-cloud-storage
google-cloud
pandas
"""

import io
from google.cloud import storage
import pandas as pd


def processiong_anfix_subscriptions(event, context):
    FILE = event
    FILE_NAME = FILE['name']
    print(f"Processing file: {FILE_NAME}.")

    STORAGE_CLIENT = storage.Client(project='kc-proyecto-mel-afx')
    BUCKET_RAW = STORAGE_CLIENT.bucket('kc-mel-practica-raw')
    BUCKET_DATA_LAKE = STORAGE_CLIENT.bucket('kc-mel-practica-datalake')

    NAME_FILE_RESULT = 'DataSet_anfix_subscriptions.csv'
    NAME_FILE_ANFIXDB = 'DataSet_anfixdb_subscription.csv'
    new_columns_df_anfix = ['score_nps', 'semaforo', 'addon_sto', 'addon_pro']

    # si hemos subido el archivo correcto
    if NAME_FILE_ANFIXDB == FILE_NAME:

        print("- Cargamos el DataFrame con los datos de suscripciones en AnfixDb importados del csv.")
        blob_file_csv = BUCKET_RAW.blob(FILE_NAME)
        data = blob_file_csv.download_as_string()
        df_anfix_db = pd.read_csv(io.BytesIO(data))
        print("df:{}".format(df_anfix_db))
        print("df.columns:{}".format(df_anfix_db.columns))
        print("The number of columns:{}".format(len(df_anfix_db.columns)))
        df_anfix = pd.DataFrame(columns=df_anfix_db.columns.union(new_columns_df_anfix))

        print('- Nos vamos a quedar con suscripción mas reciente de la compañia en Anfix.')
        companyIds = df_anfix_db.companyId.unique().tolist()
        for companyId in companyIds:
            new_row = df_anfix_db.loc[df_anfix_db['companyId'] == companyId].iloc[-1]
            new_row['score_nps'] = ''
            new_row['semaforo'] = ''
            new_row['addon_sto'] = ''
            new_row['addon_pro'] = ''
            df_anfix.loc[len(df_anfix.index)] = new_row

        print("df:{}".format(df_anfix))
        print("df.columns:{}".format(df_anfix.columns))
        print("The number of columns:{}".format(len(df_anfix.columns)))

        blob_file_result = BUCKET_DATA_LAKE.blob(NAME_FILE_RESULT)
        blob_file_result.upload_from_string(df_anfix.to_csv(), 'text/csv')

        print("- Archivo {a} almacenado correctamente en el Data Lake {b}".format(a=NAME_FILE_RESULT, b=BUCKET_DATA_LAKE))

    else:
        print("No hemos subido el archivo correcto.")
