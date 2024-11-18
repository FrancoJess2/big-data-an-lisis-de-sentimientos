import boto3
from dataclasses import dataclass

# Inicializar recurso DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('bd-sentimientos')

@dataclass
class Sentimientos:
    codigo: str
    clasificacion_texto: str
    fecha: str
    hora: str           
    periodo: str       
    idioma: str
    longitud_palabras: int
    mixto: float
    negativo: float
    neutral: float
    positivo: float
    sentimiento: str
    texto: str
    palabras_positivas: list  # Nueva columna para las palabras positivas

    @staticmethod
    def from_dynamodb(item):
        return Sentimientos(
            codigo=item.get('codigo'),
            clasificacion_texto=item.get('clasificacion_texto'),
            fecha=item.get('fecha'),
            hora=item.get('hora'),         
            periodo=item.get('periodo'),   
            idioma=item.get('idioma'),
            longitud_palabras=item.get('longitud_palabras'),
            mixto=item.get('mixto'),
            negativo=item.get('negativo'),
            neutral=item.get('neutral'),
            positivo=item.get('positivo'),
            sentimiento=item.get('sentimiento'),
            texto=item.get('texto'),
            palabras_positivas=item.get('palabras_positivas', [])  # Usar el campo si ya existe
        )

# Función para extraer palabras positivas en inglés y español
def extract_positive_words(text):
    positive_words = [
        # Palabras en inglés
        "thank", "excellent", "wonderful", "great", "amazing", "fantastic",
        "awesome", "positive", "love", "appreciate", "perfect", "happy",
        "good", "helpful", "kind",
        # Palabras en español
        "gracias", "excelente", "maravilloso", "genial", "increíble", "fantástico",
        "positivo", "amor", "aprecio", "perfecto", "feliz", "bueno", "amable", "ayuda", "asombroso"
    ]
    words = text.lower().split()
    return [word for word in words if word in positive_words]

# Función para actualizar registros en DynamoDB
def update_dynamodb_item(item):
    table.update_item(
        Key={'codigo': item['codigo']},  # Usa la clave primaria de tu tabla
        UpdateExpression="SET palabras_positivas = :p",
        ExpressionAttributeValues={
            ':p': item['palabras_positivas']
        }
    )

# Handler principal de Lambda
def lambda_handler(event, context):
    response = table.scan()
    items = response.get('Items', [])

    # Iterar sobre los elementos y manejar palabras positivas
    for item in items:
        if not item.get('palabras_positivas'):  # Si no existe la clave, extraerlas dinámicamente
            item['palabras_positivas'] = extract_positive_words(item['texto'])
            update_dynamodb_item(item)  # Actualizar DynamoDB con las palabras positivas

    # Convertir los datos en objetos de la clase Sentimientos
    objetos = [Sentimientos.from_dynamodb(item) for item in items]

    # Devolver la lista en formato JSON
    return [objeto.__dict__ for objeto in objetos]
