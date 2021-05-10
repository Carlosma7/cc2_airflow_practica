from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import pandas
import pymongo

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['carlos7ma@correo.ugr.es'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='2practicaCC',
    default_args = default_args,
    description = 'Practica 2 de Cloud Computing',
    dagrun_timeout=timedelta(minutes=60),
    schedule_interval=timedelta(days=1),
)

# Tarea 1: Crear carpeta
CrearCarpeta = BashOperator(
    task_id='CrearCarpeta',
    depends_on_past=False,
    bash_command='mkdir -p /tmp/practica/',
    dag=dag
)

# Tarea 2: Descargar fichero CSV de temperatura en la carpeta
DescargarTemperatura = BashOperator(
    task_id='DescargarTemperatura',
    depends_on_past=False,
    bash_command='curl -o /tmp/practica/temperature.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip',
    dag=dag
)

# Tarea 3: Descargar fichero CSV de humedad en la carpeta
DescargarHumedad = BashOperator(
    task_id='DescargarHumedad',
    depends_on_past=False,
    bash_command='curl -o /tmp/practica/humidity.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip',
    dag=dag
)

# Tarea 4: Descomprimir ficheros CSV
DescomprimirZips = BashOperator(
    task_id='DescomprimirZips',
    depends_on_past=False,
    bash_command='cd /tmp/practica/ && unzip -f /tmp/practica/humidity.csv.zip && unzip -f /tmp/practica/temperature.csv.zip',
    dag=dag
)

# Tarea 5: Preprocesamiento de datos
def preprocesamiento():
    # Obtener ficheros CSV con pandas
    temperature_csv = pandas.read_csv('/tmp/practica/temperature.csv')
    humidity_csv = pandas.read_csv('/tmp/practica/humidity.csv')
    # Obtener datos de San Francisco
    temperatura_sanf = temperature_csv['San Francisco']
    humedad_sanf = humidity_csv['San Francisco']
    fecha = humidity_csv['datetime']
    # Conformar datos
    columnas = {'DATE': fecha, 'TEMP': temperatura_sanf, 'HUM': humedad_sanf}
    # Conformar dataframe de datos
    datos = pandas.DataFrame(data=columnas)
    # Imputación de datos perdidos usando la media
    datos.fillna(datos.mean())
    # Exportar en CSV
    datos.to_csv('/tmp/practica/san_francisco.csv', encoding='utf-8', sep='\t', index=False)

PreprocesarDatos = PythonOperator(
    task_id='PreprocesarDatos',
    python_callable=preprocesamiento,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Tarea 6: Almacenar información en MongoDB
def almacenar():
    datos = pandas.read_csv('/tmp/practica/san_francisco.csv', sep='\t')
    # Parsing a diccionario
    datos = datos.to_dict('records')
    # Conectar cliente de mongo
    client = pymongo.MongoClient("mongodb+srv://"+"CC2"+":"+"UGR"+"@grandquiz.fo6ph.mongodb.net/CC2?retryWrites=true&w=majority")
    # Inserción datos
    code = client.CC2['SanFrancisco'].insert_many(datos)

AlmacenarDatos = PythonOperator(
    task_id='AlmacenarDatos',
    python_callable=almacenar,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Tarea 7: Descargar servicios de GitHub
DescargarV1 = BashOperator(
    task_id='DescargarV1',
    depends_on_past=False,
    bash_command='cd /tmp/practica/ && rm -rf cc2_airflow_arima_v1 && git clone https://github.com/Carlosma7/cc2_airflow_arima_v1.git',
    dag=dag
)


DescargarV2 = BashOperator(
    task_id='DescargarV2',
    depends_on_past=False,
    bash_command='cd /tmp/practica/ && rm -rf cc2_airflow_arima_v2 && git clone https://github.com/Carlosma7/cc2_airflow_arima_v2.git',
    dag=dag
)

# Tarea 8: Testear los servicios
TestV1 = BashOperator(
    task_id='TestV1',
    depends_on_past=False,
    bash_command='cd /tmp/practica/cc2_airflow_arima_v1/ && pytest test.py',
    dag=dag
)

TestV2 = BashOperator(
    task_id='TestV2',
    depends_on_past=False,
    bash_command='cd /tmp/practica/cc2_airflow_arima_v2/ && pytest test.py',
    dag=dag
)

# Tarea 9: Copiar fuentes en máquina a desplegar
CopiarFuentesV1 = BashOperator(
    task_id='CopiarFuentesV1',
    depends_on_past=False,
    bash_command='scp -r /tmp/practica/cc2_airflow_arima_v1 root@165.22.83.184:~',
    dag=dag
)

CopiarFuentesV2 = BashOperator(
    task_id='CopiarFuentesV2',
    depends_on_past=False,
    bash_command='scp -r /tmp/practica/cc2_airflow_arima_v2 root@165.22.83.184:~',
    dag=dag
)

# Tarea 10: Crear el contenedor en la máquina a desplegar
CrearDockerV1 = BashOperator(
    task_id='CrearDockerV1',
    depends_on_past=False,
    bash_command='ssh root@165.22.83.184 "cd cc2_airflow_arima_v1 && docker build --no-cache -t carlosma7/cc2_airflow_arima_v1 -f Dockerfile . "',
    dag=dag
)

CrearDockerV2 = BashOperator(
    task_id='CrearDockerV2',
    depends_on_past=False,
    bash_command='ssh root@165.22.83.184 "cd cc2_airflow_arima_v2 && docker build --no-cache -t carlosma7/cc2_airflow_arima_v2 -f Dockerfile . "',
    dag=dag
)

# Tarea 11: Desplegar
DesplegarV1 = BashOperator(
    task_id='DesplegarV1',
    depends_on_past=False,
    bash_command='ssh root@165.22.83.184 "docker run -p 2020:2020 -d carlosma7/cc2_airflow_arima_v1" ',
    dag=dag
)

DesplegarV2 = BashOperator(
    task_id='DesplegarV2',
    depends_on_past=False,
    bash_command='ssh root@165.22.83.184 "docker run -p 2021:2021 -d carlosma7/cc2_airflow_arima_v2" ',
    dag=dag
)


# Ciclo de tareas del DAG
CrearCarpeta >> DescargarTemperatura >> DescargarHumedad >> DescomprimirZips
DescomprimirZips >> PreprocesarDatos >> AlmacenarDatos >> [DescargarV1, DescargarV2]
DescargarV1 >> TestV1 >> CopiarFuentesV1 >> CrearDockerV1 >> DesplegarV1
DescargarV2 >> TestV2 >> CopiarFuentesV2 >> CrearDockerV2 >> DesplegarV2
