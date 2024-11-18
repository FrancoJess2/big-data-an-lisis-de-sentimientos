import boto3
import json
import os
import urllib.parse
import csv
import codecs
from datetime import datetime

s3 = boto3.client('s3')
comprehend = boto3.client('comprehend')

def lambda_handler(event, context):
    # Verificar si el evento proviene de EventBridge o de S3 directamente
    if 'detail' in event:  # Cuando el evento es de EventBridge
        bucket = event['detail']['bucket']['name']
        key = urllib.parse.unquote_plus(event['detail']['object']['key'])
        audio_duration = event['detail'].get('audio_duration', 'Desconocido')  # Obtener la duración del evento
    else:  # Cuando el evento viene directamente de S3
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        audio_duration = 'Desconocido'  # Si no tienes duración, puedes dejarlo como 'Desconocido'

    # Descargar el archivo CSV del bucket S3
    csv_data = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    
    # Decodificar el CSV eliminando el BOM si existe
    csv_data = codecs.decode(csv_data, 'utf-8-sig')
    
    # Detectar el idioma automáticamente usando AWS Comprehend
    language_response = comprehend.detect_dominant_language(Text=csv_data)
    language_code = language_response['Languages'][0]['LanguageCode']
    
    # Realizar el análisis de sentimientos con AWS Comprehend
    sentiment_response = comprehend.detect_sentiment(
        Text=csv_data,
        LanguageCode=language_code  # Usar el idioma detectado automáticamente
    )
    
    # Preparar los resultados del análisis de sentimientos para subirlos a S3 en JSON
    sentiment_result = json.dumps(sentiment_response)
    output_key_json = f"resultados/{os.path.basename(key).replace('.csv', '-sentimientos.json')}"
    
    # Subir los resultados al bucket S3 en formato JSON
    s3.put_object(
        Bucket='proy-bd-data-sentimientos',  # Cambia esto si tu bucket tiene otro nombre
        Key=output_key_json,
        Body=sentiment_result
    )
    
    # Crear el archivo CSV con los resultados de sentimientos
    sentiment = sentiment_response['Sentiment']
    sentiment_scores = sentiment_response['SentimentScore']
    
    # Obtener la fecha actual
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Crear el contenido CSV con las nuevas columnas: Fecha, Idioma, Texto, Sentimiento, puntajes y Duración del audio
    csv_output = [
        ['Fecha', 'Idioma', 'Texto', 'Sentimiento', 'Positivo', 'Negativo', 'Neutral', 'Mixto'],
        [current_date, language_code, csv_data, sentiment, sentiment_scores['Positive'], sentiment_scores['Negative'], sentiment_scores['Neutral'], sentiment_scores['Mixed']]
    ]
    
    # Generar el archivo CSV en el directorio temporal de Lambda
    output_key_csv = f"resultados/{os.path.basename(key).replace('.csv', '-sentimientos.csv')}"
    output_path_csv = f"/tmp/{os.path.basename(output_key_csv)}"
    
    # Escribir el archivo CSV en el directorio temporal
    with open(output_path_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(csv_output)
    
    # Subir el archivo CSV al bucket S3
    s3.upload_file(output_path_csv, 'proy-bd-data-sentimientos', output_key_csv)
    
    # Eliminar el archivo JSON después de que el CSV se haya generado y subido
    s3.delete_object(
        Bucket='proy-bd-data-sentimientos',  # Cambia esto si tu bucket tiene otro nombre
        Key=output_key_json
    )
    
    # Retornar la respuesta cuando el análisis ha sido exitoso
    return {
        'statusCode': 200,
        'body': f'Resultados guardados en JSON ({output_key_json}) y CSV ({output_key_csv}), JSON eliminado.'
    }