import boto3

def lambda_handler(event, context):
    glue_client = boto3.client('glue')

    # Verificar si 'Records' está en el evento
    if 'Records' in event and event['Records']:
        file_key = event['Records'][0]['s3']['object']['key']
        bucket_name = event['Records'][0]['s3']['bucket']['name']

        # Filtrar solo archivos en la carpeta 'resultados/' y con extensión .csv
        if file_key.startswith('resultados/') and file_key.endswith('.csv'):
            # Iniciar el Glue Job con argumentos
            response = glue_client.start_job_run(
                JobName='job-bd-etl-sentimientos',  # Reemplazar con el nombre de tu Glue Job
                Arguments={
                    '--s3_input_file': f's3://{bucket_name}/{file_key}',  # Pasar la ruta del archivo como argumento
                }
            )

            return {
                'statusCode': 200,
                'body': 'Glue Job started successfully with file: ' + file_key
            }
        else:
            return {
                'statusCode': 400,
                'body': 'El archivo no está en la carpeta resultados o no es un .csv: ' + file_key
            }
    else:
        return {
            'statusCode': 400,
            'body': 'Error: Event does not contain Records.'
        }
