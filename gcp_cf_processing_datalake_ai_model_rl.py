import io
from google.cloud import storage
import pandas as pd

import unicodedata

import nltk
from nltk import ngrams
from nltk import RegexpTokenizer
from nltk.probability import FreqDist
from nltk.stem import WordNetLemmatizer

from num2words import num2words

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction import _stop_words
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_selection import chi2
from sklearn.ensemble import GradientBoostingClassifier

from tensorflow.keras.layers import (
    Dense,
    Dropout,
    Embedding,
    LSTM,
)
from tensorflow.keras.models import Sequential
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.optimizers import Adam

from keras.preprocessing import sequence

nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')

# Método eliminación de acentos, etc
def regularize_unicode(text):
  clean_text = ''
  clean_text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
  return clean_text

# Método para eliminar stop words
def delete_stop_words(text, sw_list=nltk.corpus.stopwords.words('spanish')):
  clean_text = ''
  clean_text = ' '.join([word for word in text.lower().split() if word not in sw_list])
  return clean_text


# Método para regularizar en mayusculas o minusculas
def regularize_lowercase(text, tokenizer):
  clean_text = list()

  for word in tokenizer.tokenize(text):
    clean_word = word.lower()
    clean_text.append(clean_word)

  return ' '.join(clean_text)


# Método lematizar
def regularize_lemat(text, tokenizer, lemmatizer):
  clean_text = list()

  for word in tokenizer.tokenize(text):
    clean_word = lemmatizer.lemmatize(word)
    clean_text.append(clean_word)

  return ' '.join(clean_text)


# Método para eliminar espacios sobrantes
import string
def regularize_spaces(text):
  table = str.maketrans('', '', string.punctuation)
  clean_text = ' '.join([word.translate(table) for word in text.split()])
  return clean_text


# Método convertir dígitos a palabras
def regularize_convert_digits_to_words(text, tokenizer):
  clean_text = list()

  for word in tokenizer.tokenize(text):
    if word.isdigit():
      clean_word = num2words(word, lang='es')
      clean_text.append(clean_word)
    else:
      clean_text.append(word)

  return ' '.join(clean_text)


# Metodo por el que a partir de la puntuación lo convertimos en semaforo
def label_semaphore(row):
  if int(row['score']) <= 6:
    return 1
  else:
    return 0


# Método de procesamiento general del texto
def pipeline_cleaner(text_list, stopword_es):

  clean_text = list()
  tokenizer = RegexpTokenizer(r'\w+')
  lemmatizer = WordNetLemmatizer()

  for i in range(len(text_list)):
    text_clean = regularize_unicode(text_list[i])
    text_clean = regularize_lowercase(text_clean, tokenizer)
    text_clean = delete_stop_words(text_clean, stopword_es)
    text_clean = regularize_lemat(text_clean, tokenizer, lemmatizer)
    text_clean = regularize_convert_digits_to_words(text_clean, tokenizer)
    text_clean = regularize_spaces(text_clean)
    clean_text.append(text_clean)

  return clean_text

def processing_datalake_ai_model_rl(event, context):

    FILE = event
    FILE_NAME = FILE['name']
    print(f"Processing file: {FILE_NAME}.")

    STORAGE_CLIENT = storage.Client(project='kc-proyecto-mel-afx')
    BUCKET_DATA_LAKE_PROCESSED = STORAGE_CLIENT.bucket('kc-mel-practica-datalake-processed')

    NAME_FILE = 'result_anfix_clients_pro_nps.csv'
    NAME_FILE_CLIENT = 'result_anfx_clients_pro.csv'
    NAME_FILE_RESULT = 'result_anfix_clients_ai.csv'
    NAME_FILE_NPS = 'result_anfix_clients_nps_ai.csv'

    if NAME_FILE == FILE_NAME:

        print("- Cargamos el DataFrame con los datos a trabajar con el modelo.")
        blob_file_csv = BUCKET_DATA_LAKE_PROCESSED.blob(FILE_NAME)
        data = blob_file_csv.download_as_string()
        df_client = pd.read_csv(io.BytesIO(data))
        print("df:{}".format(df_client))
        print("df.columns:{}".format(df_client.columns))
        print("The number of columns:{}".format(len(df_client.columns)))

        # Lista con las Stop Words
        print("- Generamos la lista con las Stop Words.")
        stopword_es = nltk.corpus.stopwords.words('spanish')
        stopword_es.append('1o')
        stopword_es.append('2o')
        df_client_clean = df_client[df_client['nps-feedback'].notna()]

        print("- Procesamos los datos de feedback para el modelo.")
        nps_feedback = pipeline_cleaner(df_client_clean['nps-feedback'].tolist(), stopword_es)
        print("- Procesamos los datos de score para el modelo.")
        nps_score = df_client_clean['nps-score']

        df_model = pd.DataFrame({
            'feedback': nps_feedback,
            'score': nps_score
        })
        df_model.dropna(subset=['feedback', 'score'], inplace=True)
        df_model.reset_index(drop=True, inplace=True)

        print("- Generamos la columna para el semaforo.")
        df_model["semaphore"] = df_model.apply(lambda row: label_semaphore(row), axis=1)

        X_train, X_test, y_train, y_test = train_test_split(
            df_model['feedback'],
            df_model['semaphore'],
            train_size=0.90,
            test_size=0.10,
            random_state=42,
            shuffle=True
        )
        cv = CountVectorizer(
            max_df=0.95,
            min_df=1,
            max_features=1000,
            strip_accents='ascii',
            ngram_range=(1, 3)
        )
        cv.fit(X_train)
        # aplicamos el modelo entrenado a los datos para obtener su representacion
        X_train_ = cv.transform(X_train)
        # X_test_ = cv.transform(X_test)

        print("- Generamos el modelo de regresion logistica.")
        modelo_ok = LogisticRegression(C=1000, solver='lbfgs', max_iter=1500)
        print("- Fit del modelo de regresion logistica.")
        modelo_ok.fit(X_train_, y_train)

        # TODO: Predecir el valor del semaforo para todos los registros
        print("- Cargamos el DataFrame con los datos de las suscripciones")
        blob_file_csv = BUCKET_DATA_LAKE_PROCESSED.blob(NAME_FILE_CLIENT)
        data_anfix = blob_file_csv.download_as_string()
        df_anfix = pd.read_csv(io.BytesIO(data_anfix))
        print("The number of columns:{}".format(len(df_anfix.columns)))
        print("The number of row:{}".format(df_anfix.shape[0]))
        print("df_anfix.columns:{}".format(df_anfix.columns))

        for index, row in df_anfix.iterrows():
            print("Procesando linea {a} de {b}".format(a=index, b=df_anfix.shape[0]))
            if not pd.isna(row['nps-feedback']):
                score = modelo_ok.predict(cv.transform([row['nps-feedback']]))
                print("La linea ha obtenido un {a} en con el modelo AI para companyId: {b}.".format(a=score[0], b=row['company-id']))
                df_anfix.loc[df_anfix['company-id'] == row['company-id'], 'nps-score-semaphore'] = 'Rojo' if score[0] == 1 else 'Verde'
            else:
                print("La linea no tiene feedback para procesar")

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

        columns = ['company-id', 'company-is-actived', 'company-name',
                   'company-postal-code', 'company-province',
                   'nps-feedback', 'nps-previous-score',
                   'nps-score', 'nps-score-old', 'nps-score-semaphore',
                   'organization-type',
                   'subscription-active',
                   'subscription-end-date',
                   'subscription-expiration-date', 'subscription-id',
                   'subscription-name',
                   'subscription-start-date']
        df_nps = df_anfix[columns]
        df_nps = df_nps[~df_nps['nps-score'].isin([''])]
        df_nps = df_nps[~df_nps['nps-score'].isna()]
        df_nps.reset_index(drop=True, inplace=True)

        print("The number of columns (nps):{}".format(df_nps.shape[1]))
        print("The number of row (nps):{}".format(df_nps.shape[0]))
        print("df_nps.columns:{}".format(df_nps.columns))

        blob_file_result_nps = BUCKET_DATA_LAKE_PROCESSED.blob(NAME_FILE_NPS)
        blob_file_result_nps.upload_from_string(df_nps.to_csv(), 'text/csv')

        print("- Archivo {a} almacenado correctamente en el Data Lake {b}".format(a=NAME_FILE_NPS, b=BUCKET_DATA_LAKE_PROCESSED))

    else:
        print("No hemos subido el archivo correcto.")