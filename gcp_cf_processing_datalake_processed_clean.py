import io
from google.cloud import storage
import pandas as pd

def processing_datalake_processed_clean(event, context):

    FILE = event
    FILE_NAME = FILE['name']
    print(f"Processing file: {FILE_NAME}.")

    STORAGE_CLIENT = storage.Client(project='kc-proyecto-mel-afx')
    BUCKET_DATA_LAKE_PROCESSED = STORAGE_CLIENT.bucket('kc-mel-practica-datalake-processed')

    NAME_FILE = 'result_anfx_clients.csv'
    NAME_FILE_RESULT = 'result_anfx_clients_pro.csv'

    if NAME_FILE == FILE_NAME:
        print("- Cargamos el DataFrame con los datos a limpiar.")
        blob_file_csv = BUCKET_DATA_LAKE_PROCESSED.blob(FILE_NAME)
        data = blob_file_csv.download_as_string()
        df_anfix = pd.read_csv(io.BytesIO(data))
        print("df:{}".format(df_anfix))
        print("df.columns:{}".format(df_anfix.columns))
        print("The number of columns:{}".format(len(df_anfix.columns)))

        # eliminamos los saltos de lineas
        df_anfix['nps_feedback'] = df_anfix['nps_feedback'].str.replace(r'\n', ' ', regex=True)
        # ponemos todos los separadores que tenemos con ','
        df_anfix['rtps_component_list'] = df_anfix['rtps_component_list'].str.replace(r';', ', ', regex=True)

        df_anfix = df_anfix.fillna('')
        df_anfix['addon_pro'] = df_anfix['addon_pro'].astype('string')
        df_anfix['addon_pro'] = df_anfix['addon_pro'].str.replace(r'.0', '', regex=True)
        df_anfix['addon_pro_active'] = df_anfix['addon_pro_active'].astype('string')
        df_anfix['addon_pro_active'] = df_anfix['addon_pro_active'].str.replace(r'.0', '', regex=True)
        df_anfix['addon_pro_id'] = df_anfix['addon_pro_id'].astype('string')
        df_anfix['addon_pro_id'] = df_anfix['addon_pro_id'].str.replace(r'.0', '', regex=True)

        df_anfix['addon_sto'] = df_anfix['addon_sto'].astype('string')
        df_anfix['addon_sto'] = df_anfix['addon_sto'].str.replace(r'.0', '', regex=True)
        df_anfix['addon_sto_active'] = df_anfix['addon_sto_active'].astype('string')
        df_anfix['addon_sto_active'] = df_anfix['addon_sto_active'].str.replace(r'.0', '', regex=True)
        df_anfix['addon_sto_id'] = df_anfix['addon_sto_id'].astype('string')
        df_anfix['addon_sto_id'] = df_anfix['addon_sto_id'].str.replace(r'.0', '', regex=True)

        df_anfix['nps_score'] = df_anfix['nps_score'].astype('string')
        df_anfix['nps_score'] = df_anfix['nps_score'].str.replace(r'.0', '', regex=True)
        df_anfix['nps_prev_score'] = df_anfix['nps_prev_score'].astype('string')
        df_anfix['nps_prev_score'] = df_anfix['nps_prev_score'].str.replace(r'.0', '', regex=True)

        df_anfix['rtps_total'] = df_anfix['rtps_total'].astype('string')
        df_anfix['rtps_total'] = df_anfix['rtps_total'].str.replace(r'.0', '', regex=True)

        df_anfix.rename(columns={
            'addon_pro': 'addon-pro',
            'addon_pro_active': 'addon-pro-is-active',
            'addon_pro_end': 'addon-pro-end-date',
            'addon_pro_id': 'addon-pro-id',
            'addon_pro_start': 'addon-pro-start-date',
            'addon_sto': 'addon-sto',
            'addon_sto_active': 'addon-sto-is-active',
            'addon_sto_end': 'addon-sto-end-date',
            'addon_sto_id': 'addon-sto-id',
            'addon_sto_start': 'addon-sto-start-date',
            'companyId': 'company-id',
            'CorporateName': 'company-name',
            'EMail_company': 'company-email',
            'companyId_clean': 'company-id-clean',
            'dc_CreationDate': 'company-creation-date',
            'dc_DeletionDate': 'company-deletion-date',
            'dc_IsActived': 'company-is-actived',
            'dc_PhoneNumber': 'company-phone',
            'dir_CityName': 'company-city',
            'dir_CountryName': 'company-country',
            'dir_Door': 'company-door',
            'dir_Doorway': 'company-doorway',
            'dir_Floor': 'company-floor',
            'dir_Name': 'company-street',
            'dir_PostalCode': 'company-postal-code',
            'dir_ProvinceName': 'company-province',
            'dir_Stair': 'company-stair',
            'suscriptionId': 'subscription-id',
            'suscription_active': 'subscription-active',
            'doc_DirCommercialConfigId': 'subscription-configuration-id',
            'dcc_Name': 'subscription-name',
            'doc_EndDate': 'subscription-end-date',
            'doc_ExpirationDate': 'subscription-expiration-date',
            'doc_IsUninstalled': 'subscription-is-uninstalled',
            'doc_StartDate': 'subscription-start-date',
            'du_IsActived': 'user-is-actived',
            'du_user': 'user-name',
            'Email_acceso': 'user-access-email',
            'do_PhoneNumber': 'organization-phone',
            'organizationId': 'organization-id',
            'organizationId_clean': 'organization-id-clean',
            'type_organization': 'organization-type',
            'OrganizationName': 'organization-name',
            'EMail_organization': 'organization-email',
            'score_nps': 'nps-score-old',
            'semaforo': 'nps-score-semaphore',
            'nps_timestamp': 'nps-timestamp',
            'nps_score': 'nps-score',
            'nps_prev_score': 'nps-previous-score',
            'nps_feedback': 'nps-feedback',
            'rtps_total': 'rtps-total',
            'rtps_component_list': 'rtps-companent-list'
        }, inplace=True)
        df_anfix = df_anfix.reindex(sorted(df_anfix.columns), axis=1)

        exit_while = False
        while not exit_while:
            if df_anfix.columns[0] != 'addon-pro':
                df_anfix.drop(columns=df_anfix.columns[0], axis=1, inplace=True)
            else:
                exit_while = True

        df_anfix.reset_index(drop=True, inplace=True)
        blob_file_result = BUCKET_DATA_LAKE_PROCESSED.blob(NAME_FILE_RESULT)
        blob_file_result.upload_from_string(df_anfix.to_csv(), 'text/csv')

        print("- Archivo {a} almacenado correctamente en el Data Lake {b}".format(a=NAME_FILE_RESULT, b=BUCKET_DATA_LAKE_PROCESSED))

    else:
        print("No hemos subido el archivo correcto.")