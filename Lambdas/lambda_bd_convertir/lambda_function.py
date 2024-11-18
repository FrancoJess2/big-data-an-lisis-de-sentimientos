import json
import boto3
import urllib.parse
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['detail']['bucket']['name']
    key = event['detail']['object']['key']
    
    transcript_data = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8', errors='ignore')
    transcript_text = json.loads(transcript_data)
    
    full_text = ''
    
    for item in transcript_text['results']['items']:
        if 'alternatives' in item:
            full_text += item['alternatives'][0]['content'] + ' '
    
    csv_file_path = f"/tmp/{os.path.basename(key).replace('.json', '.csv')}"
    
    with open(csv_file_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
        csvfile.write(full_text.strip())  # Escribir solo el texto, todo en una línea
    
    output_key = f"transcriptions/{os.path.basename(csv_file_path)}"
    s3.upload_file(csv_file_path, bucket, output_key)
    
    s3.delete_object(Bucket=bucket, Key=key)
    
    return {
        'statusCode': 200,
        'body': f'Archivo CSV creado y JSON eliminado: {output_key}'
    }