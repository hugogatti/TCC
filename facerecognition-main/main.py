import cv2
import face_recognition as fr
import os
import time
import numpy as np
from PIL import Image
from psycopg2 import OperationalError
import psycopg2
import boto3
from decouple import config
import RPi.GPIO as GPIO

cap = cv2.VideoCapture(0)
preset_folder_path = './preset'
compare_time = time.time()
reload_images_time = time.time()
verify_time = time.time()
pre_set_images = []
face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
reconhecer = True
GPIO.setmode(GPIO.BOARD)
Porta1 =  3
Porta2 = 15
Porta3 = 37

s3 = boto3.client(
    service_name='s3',
    # region_name='us-east-1b',
    aws_access_key_id= config('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key= config('AWS_SECRET_ACCESS_KEY')
)
bucket = config('AWS_STORAGE_BUCKET_NAME')

def connect_db():
    try:
        connection = psycopg2.connect(
            user=config('DB_USER'),
            password=config('DB_PASSWORD'),
            host= config('DB_HOST'),
            port= config('DB_PORT'),
            database=config('DB_NAME')
        )
        print("Conexão com o banco de dados realizada com sucesso")
        return connection
    except OperationalError as e:
        print(e)
        return None
    
def disconnect_db(connection):
    if connection:
        connection.close()
        return True
    
    return False

def consult_db(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
    except OperationalError as e:
        print(e)
        return None
    finally:
        cursor.close()
    
def update_db(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        cursor.close()
        print("Atualização realizada com sucesso")
    except OperationalError as e:
        connection.rollback()
        print(e)
    finally:
        cursor.close()

def load_preset_images():
    global pre_set_images
    global preset_folder_path
    
    print("Atualizando imagens")
    bucket_files = s3.list_objects(Bucket=bucket)
    pre_set_images = os.listdir(preset_folder_path)
    
    if 'Contents' in bucket_files:
        for file in bucket_files['Contents']:
            key = file['Key']
            print(key)
            if key.startswith('media/ReconhecimentoFacial/'):
                newKey = key.split('/')[-1]
                file_path = os.path.join(preset_folder_path, newKey)
            
                if not os.path.exists(preset_folder_path):
                    s3.download_file(bucket, key, file_path)
                    print("Download executado com sucesso!")
                    
    preset_folder_path = './preset'

def configura_update(id_prescricao):
    sql = """
        UPDATE public.Prescricoes 
        SET Ativo = False
        WHERE id = '%s';
    """ % id_prescricao
    return sql

def liberar_porta(porta):
    GPIO.setup(porta, GPIO.OUT)
    
    if (porta == 1):
        GPIO.output(Porta1, GPIO.HIGH)
        time.sleep(5)
        GPIO.output(Porta1, GPIO.LOW)
    if (porta == 2):
        GPIO.output(Porta2, GPIO.HIGH)
        time.sleep(5)
        GPIO.output(Porta2, GPIO.LOW)
    if (porta == 3):
        GPIO.output(Porta3, GPIO.HIGH)
        time.sleep(5)
        GPIO.output(Porta3, GPIO.LOW)
    GPIO.cleanup()
    
def verify_face(compare_frame, id_prescricao, portas_prescricao):
    global reconhecer
    for image in pre_set_images:
        if now - compare_time > 15:
            print("Nenhuma face reconhecida")
            return
        path_image = os.path.join(preset_folder_path, image)

        preset_image = fr.load_image_file(path_image)
        preset_image = cv2.cvtColor(preset_image, cv2.COLOR_BGR2RGB)

        encode_preset = fr.face_encodings(preset_image)
        if encode_preset:
            encode_preset = encode_preset[0]

            encode_image = fr.face_encodings(compare_frame)
            if encode_image:
                encode_image = encode_image[0]
                compare = fr.compare_faces([encode_image], encode_preset)
                if compare:
                    print("Reconhecido")
                    for porta in portas_prescricao:
                        print(porta)
                        liberar_porta(porta)
                        
                    if id_prescricao != 0:
                        sql = configura_update(id_prescricao)
                        update_db(connect, sql)
                    else:
                        print("Erro ao buscar o id da prescrição.")
                        return
                    reconhecer = False
                    return
                print(compare)

load_preset_images()
connect = connect_db()
linhas = []
portas_prescricao = []
id_prescricao = 0

while cap.isOpened():
    ret, frame = cap.read()

    if not ret:
        break

    global now
    now = time.time()
    
    query = """
        SELECT m."Porta", p."id" 
        FROM "Prescricao" p
        INNER JOIN "Prescricao_Medicamentos" pm ON p.id = pm.prescricao_id
        INNER JOIN "Medicamento" m ON pm.medicamento_id = m.id
        WHERE p."Ativo" = true
        AND p."Data" = (SELECT MIN("Data") FROM "Prescricao" WHERE "Ativo" = true)
        ORDER by p."Data" ASC;
    """
    
    if now - reload_images_time > 30:
        print("Atualizando imagens")
        load_preset_images()
        reload_images_time = now

    if now - compare_time > 10:
        resultado = consult_db(connect, query)
        
        if resultado:
            reconhecer = True
            for linha in resultado:
                linhas.append(linha)
                portas_prescricao.append(linha[0])
                id_prescricao = linha[1]
        else:
            print("Nenhum resultado encontrado.")
        compare_time = now
    
    gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(gray_frame, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))
    
    frame_array = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    
    img = Image.fromarray(frame_array)
    temp_file_path = os.path.join(os.getcwd(), "temp_frame.jpg")
    img.save(temp_file_path)

    compare_frame = fr.load_image_file(temp_file_path)
    os.remove(temp_file_path)
    compare_frame = cv2.cvtColor(compare_frame, cv2.COLOR_BGR2RGB)

    if len(fr.face_locations(compare_frame)) > 0:
        face_loc = fr.face_locations(compare_frame)[0]
        cv2.rectangle(compare_frame, (face_loc[3], face_loc[0], face_loc[1], face_loc[2]), (0, 255, 0), 2)
    
    if len(faces) > 0 and reconhecer == True and id_prescricao != 0:
        verify_face(compare_frame, id_prescricao)

    cv2.imshow("teste", frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

    portas_prescricao = []

cap.release()
cv2.destroyAllWindows()
