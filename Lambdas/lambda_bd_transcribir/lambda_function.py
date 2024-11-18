import boto3
import time

transcribe = boto3.client('transcribe')

def lambda_handler(event, context):
    
    bucket = event['detail']['bucket']['name']
    key = event['detail']['object']['key']
    
    job_name = key.split('.')[0] + '-' + str(int(time.time()))
    
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': f's3://{bucket}/{key}'},
        MediaFormat='mp3',
        IdentifyLanguage=True,  # Habilitar la detección automática del idioma
        OutputBucketName='proy-bd-audios-salida'
    )
    
    return {
        'statusCode': 200,
        'body': f'Transcripción iniciada: {job_name}'
    }