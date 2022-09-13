import io
from google.cloud import storage
import pandas as pd


def processing_subscriptions(df_subscriptions, df_result):
    df_anfix = df_result
    count = 1

    # procesamos las suscripciones principales
    for index, row in df_subscriptions.iterrows():
        row_suscriptions = df_anfix.loc[df_anfix['companyId'] == row['companyId']]
        if len(row_suscriptions.index) == 0:
            df_anfix.loc[len(df_anfix.index)] = row
        else:
            df_anfix.loc[df_anfix['companyId'] == row['companyId'],
                         ['CorporateName', 'EMail_company', 'EMail_organization', 'Email_acceso', 'OrganizationName',
                          'companyId', 'companyId_clean', 'dc_CreationDate', 'dc_DeletionDate', 'dc_IsActived',
                          'dc_PhoneNumber', 'dcc_Name', 'dir_CityName', 'dir_CountryName', 'dir_Door', 'dir_Doorway',
                          'dir_Floor', 'dir_Name', 'dir_PostalCode', 'dir_ProvinceName', 'dir_Stair', 'do_PhoneNumber',
                          'doc_DirCommercialConfigId', 'doc_EndDate', 'doc_ExpirationDate', 'doc_IsUninstalled',
                          'doc_StartDate', 'du_IsActived', 'du_user', 'organizationId', 'organizationId_clean',
                          'suscriptionId', 'suscription_active', 'type_organization']
            ] = [row['CorporateName'], row['EMail_company'], row['EMail_organization'], row['Email_acceso'],
                 row['OrganizationName'], row['companyId'],
                 row['companyId_clean'], row['dc_CreationDate'], row['dc_DeletionDate'], row['dc_IsActived'],
                 row['dc_PhoneNumber'], row['dcc_Name'], row['dir_CityName'], row['dir_CountryName'],
                 row['dir_Door'], row['dir_Doorway'], row['dir_Floor'], row['dir_Name'], row['dir_PostalCode'],
                 row['dir_ProvinceName'], row['dir_Stair'], row['do_PhoneNumber'],
                 row['doc_DirCommercialConfigId'], row['doc_EndDate'], row['doc_ExpirationDate'],
                 row['doc_IsUninstalled'], row['doc_StartDate'], row['du_IsActived'], row['du_user'],
                 row['organizationId'], row['organizationId_clean'],
                 row['suscriptionId'], row['suscription_active'], row['type_organization']]

        print("Procesado principal {a} de {b}.".format(a=count, b=df_subscriptions.shape[0]))
        count += 1
    return df_anfix


def processing_project(df_projects, df_result):
    df_anfix = df_result
    count = 1

    # procesamos las suscripciones de proyectos
    for index, row in df_projects.iterrows():
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'addon_pro'] = 1
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'addon_pro_active'] = int(row["suscription_active"])
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'addon_pro_id'] = row['doc_DirCommercialConfigId']
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'addon_pro_start'] = row['doc_StartDate']
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'addon_pro_end'] = row['doc_EndDate']

        print("Procesado proyectos {a} de {b}.".format(a=count, b=df_projects.shape[0]))
        count += 1
    return df_anfix


def processing_stock(df_stock, df_result):
    df_anfix = df_result
    count = 1
    # procesamos las suscripciones de stock
    for index, row in df_stock.iterrows():
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'addon_sto'] = 1
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'addon_sto_active'] = int(row["suscription_active"])
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'addon_sto_id'] = row['doc_DirCommercialConfigId']
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'addon_sto_start'] = row['doc_StartDate']
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'addon_sto_end'] = row['doc_EndDate']

        print("Procesado stock {a} de {b}.".format(a=count, b=df_stock.shape[0]))
        count += 1
    return df_anfix


def processing_nps(df_nps, df_result):
    df_anfix = df_result
    count = 1

    # procesamos las puntuaciones nps
    for index, row in df_nps.iterrows():
        print("Procesado nps {a} de {b} con index {c}.".format(a=count, b=df_nps.shape[0], c=index))
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'nps_timestamp'] = row["timestamp"]
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'nps_score'] = row["score"]
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'nps_prev_score'] = row['prev_nps_score']
        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'nps_feedback'] = row['feedback']

        semaphore = ''
        if 0 <= row['score'] <= 5:
            semaphore = 'Rojo'
        elif 6 <= row['score'] <= 8:
            semaphore = 'Amarillo'
        elif 9 <= row['score'] <= 10:
            semaphore = 'Verde'

        df_anfix.loc[df_anfix['companyId'] == row['companyId'], 'semaforo'] = semaphore
        count += 1
    return df_anfix


def processing_rtps(df_rtps, df_result):
    df_anfix = df_result
    count = 1

    # procesamos las entradas de rtps
    for index, row in df_rtps.iterrows():
        print("Procesado rtps {a} de {b}.".format(a=count, b=df_rtps.shape[0]))
        df_anfix.loc[df_anfix['EMail_company'] == row['mail'], 'rtps_total'] = row["total_rtps"]
        df_anfix.loc[df_anfix['EMail_company'] == row['mail'], 'rtps_component_list'] = row["component_list"]

        df_anfix.loc[df_anfix['EMail_organization'] == row['mail'], 'rtps_total'] = row["total_rtps"]
        df_anfix.loc[df_anfix['EMail_organization'] == row['mail'], 'rtps_component_list'] = row["component_list"]

        df_anfix.loc[df_anfix['Email_acceso'] == row['mail'], 'rtps_total'] = row["total_rtps"]
        df_anfix.loc[df_anfix['Email_acceso'] == row['mail'], 'rtps_component_list'] = row["component_list"]

        count += 1
    return df_anfix


def error():
    print("No hemos subido el archivo para este tipo de proceso.")


def processing_datalake_generate_client(event,context):
    FILE = event
    FILE_NAME = FILE['name']
    print(f"Processing file: {FILE_NAME}.")

    STORAGE_CLIENT = storage.Client(project='kc-proyecto-mel-afx')
    BUCKET_DATA_LAKE = STORAGE_CLIENT.bucket('kc-mel-practica-datalake')
    BUCKET_DATA_LAKE_PROCESSED = STORAGE_CLIENT.bucket('kc-mel-practica-datalake-processed')

    NAME_FILE_SUBSCRIPTIONS = 'DataSet_anfix_subscriptions.csv'
    NAME_FILE_PROJECTS = 'DataSet_anfix_projects.csv'
    NAME_FILE_STOCK = 'DataSet_anfix_stock.csv'
    NAME_FILE_NPS = 'DataSet_appcues_nps.csv'
    NAME_FILE_RTPS = 'DataSet_jira_rtps.csv'
    NAME_FILE_ANFIX = 'result_anfx_clients.csv'

    if FILE_NAME in (NAME_FILE_SUBSCRIPTIONS, NAME_FILE_PROJECTS, NAME_FILE_STOCK, NAME_FILE_NPS, NAME_FILE_RTPS):

        # cargamos el archivo que será el resultado
        print("- Cargamos el DataFrame con la información que ya tenemos como resultado csv.")
        blob_file_result = BUCKET_DATA_LAKE_PROCESSED.blob(NAME_FILE_ANFIX)
        data_result = blob_file_result.download_as_string()
        df_result = pd.read_csv(io.BytesIO(data_result))
        print("The number of rows result: {}".format(df_result.shape[0]))

        if FILE_NAME == NAME_FILE_SUBSCRIPTIONS:
            print("- Vamos actualizar la información de las suscripciones principales.")
            blob_file_subscriptions = BUCKET_DATA_LAKE.blob(FILE_NAME)
            data_subscriptions = blob_file_subscriptions.download_as_string()
            df_subscriptions = pd.read_csv(io.BytesIO(data_subscriptions))
            print("The number of rows subscriptions: {}".format(df_subscriptions.shape[0]))
            df_result = processing_subscriptions(df_subscriptions, df_result)
        elif FILE_NAME == NAME_FILE_PROJECTS:
            print("- Vamos actualizar la información de las suscripciones de proyectos.")
            blob_file_projects = BUCKET_DATA_LAKE.blob(FILE_NAME)
            data_projects = blob_file_projects.download_as_string()
            df_projects = pd.read_csv(io.BytesIO(data_projects))
            print("The number of rows projects: {}".format(df_projects.shape[0]))
            df_result = processing_project(df_projects, df_result)
        elif FILE_NAME == NAME_FILE_STOCK:
            print("- Vamos actualizar la información de las suscripciones de stock.")
            blob_file_stock = BUCKET_DATA_LAKE.blob(FILE_NAME)
            data_stock = blob_file_stock.download_as_string()
            df_stock = pd.read_csv(io.BytesIO(data_stock))
            print("The number of rows stock: {}".format(df_stock.shape[0]))
            df_result = processing_stock(df_stock, df_result)
        elif FILE_NAME == NAME_FILE_NPS:
            print("- Vamos actualizar la información del NPS que nos ha facilitado el cliente.")
            blob_file_nps = BUCKET_DATA_LAKE.blob(FILE_NAME)
            data_nps = blob_file_nps.download_as_string()
            df_nps = pd.read_csv(io.BytesIO(data_nps))
            print("The number of rows nps: {}".format(df_nps.shape[0]))
            df_result = processing_nps(df_nps, df_result)
        elif FILE_NAME == NAME_FILE_RTPS:
            print("- Vamos actualizar la información de los RTPs que tiene el cliente en el sistema.")
            blob_file_rtps = BUCKET_DATA_LAKE.blob(FILE_NAME)
            data_rtps = blob_file_rtps.download_as_string()
            df_rtps = pd.read_csv(io.BytesIO(data_rtps))
            print("The number of rows rtps: {}".format(df_rtps.shape[0]))
            df_result = processing_rtps(df_rtps, df_result)

        df_result.reset_index(drop=True, inplace=True)
        # almacenamos el fichero de nuevo con las modificaciones encontradas
        blob_file_result.upload_from_string(df_result.to_csv(), 'text/csv')
        print("The number of rows result: {}".format(df_result.shape[0]))
        print("- Archivo {a} almacenado correctamente en el Data Lake Processed {b}".format(a=NAME_FILE_ANFIX, b=BUCKET_DATA_LAKE_PROCESSED))

    else:
        error()