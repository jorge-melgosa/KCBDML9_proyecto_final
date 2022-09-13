"""
Que vamos a hacer:
Vamos a procesar el archivo donde tenemos almacenados los incidencias de los clientes
No tenemos ids, por lo que utilizaremos el correo como identificador unico

Atributos que vamos a almacenar
- mail (Email propietario)
- company (Empresa)
- total_rtps (Número de incidencias abiertas)
- component_list (Componentes)(Componentes afectados unicos)

requirements
-----------------------
google-cloud-storage
google-cloud
pandas
"""

import io
from google.cloud import storage
import pandas as pd
from numpy import nan


def processiong_anfix_addons_subscriptions(event, context):
    FILE = event
    FILE_NAME = FILE['name']
    print(f"Processing file: {FILE_NAME}.")

    STORAGE_CLIENT = storage.Client(project='kc-proyecto-mel-afx')
    BUCKET_RAW = STORAGE_CLIENT.bucket('kc-mel-practica-raw')
    BUCKET_DATA_LAKE = STORAGE_CLIENT.bucket('kc-mel-practica-datalake')

    NAME_FILE_RESULT = 'DataSet_jira_rtps.csv'
    NAME_FILE_RTPS = 'DataSet_jira_rtps.csv'

    columns_rtps = [
        'mail',
        'company',
        'total_rtps',
        'component_list'
    ]

    # si hemos subido el archivo correcto
    if NAME_FILE_RTPS == FILE_NAME:

        print("- Cargamos el DataFrame con los datos de rtps que JIRA recoge.")
        blob_file_csv = BUCKET_RAW.blob(FILE_NAME)
        data = blob_file_csv.download_as_string()
        df_rtps_raw = pd.read_csv(io.BytesIO(data))
        print("df:{}".format(df_rtps_raw))
        print("df.columns:{}".format(df_rtps_raw.columns))
        print("The number of columns:{}".format(len(df_rtps_raw.columns)))
        df_rtps = pd.DataFrame(columns=columns_rtps)

        print('- Nos vamos a quedar con los mails unicos para ir procesando la información')
        mail_list = df_rtps_raw['Email Propietario'].unique().tolist()
        mail_list = [item for item in mail_list if not (pd.isnull(item)) == True]

        for mail_company in mail_list:
            df_new = df_rtps_raw.loc[df_rtps_raw['Email Propietario'] == mail_company]
            df_new = df_new.fillna('')
            print("Procesando la empresa: {}".format(mail_company))
            new_row = {
                'mail': mail_company,
                'company': df_new['Empresa'].unique().tolist()[0],
                'total_rtps': df_new.shape[0],
                'component_list': ', '.join(df_new['Componentes'].unique().tolist())
            }
            df_rtps.loc[len(df_rtps.index)] = new_row

        print("df:{}".format(df_rtps))
        print("df.columns:{}".format(df_rtps.columns))
        print("The number of columns:{}".format(len(df_rtps.columns)))

        df_rtps.reset_index(drop=True, inplace=True)
        blob_file_stock_result = BUCKET_DATA_LAKE.blob(NAME_FILE_RESULT)
        blob_file_stock_result.upload_from_string(df_rtps.to_csv(), 'text/csv')
        print("- Archivo {a} almacenado correctamente en el Data Lake {b}".format(a=NAME_FILE_RESULT, b=BUCKET_DATA_LAKE))

    else:
        print("No hemos subido el archivo correcto.")