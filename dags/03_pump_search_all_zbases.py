import datetime
import os
import requests
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine, MetaData, text, delete, insert, Table, Column, Integer, String
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow import settings
from airflow.models.baseoperator import chain

# Список баз данных, которые требуется обработать
BASES = [15, 20, 27, 29, 30, 43, 56, 71, 73]

# Ограничение на количество одновременных подключений к удаленному серверу
CHANNELS = 4

LOCAL_RECEIVE_TABLE = 'NANIM'
DAG_ID = "03.Pump_search_all_Zbases"

columns_in = ('LS', 'FIO', 'CLOSE_LS', 'ADDRESS')
columns_out = ('BASE', 'LS', 'FIO', 'ADDRESS', 'CLOSE_LS')

default_args = {
    'owner': 'MordanovDA',
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2024, month=4, day=18).in_timezone('Europe/Moscow'),
    'email': ['mordanov.da@rkc43.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5)
}

@dag(
    dag_id=DAG_ID,
    schedule="10 3 * * 0-5",
#    schedule=none,
    start_date=pendulum.datetime(2024, 3, 26, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    description="Загрузка информации для поиска по всем базам Злобина",
    default_args=default_args,
)

def Pump_all_Zbases():
    tasks = []
# ==============================================================
    @task(task_id='start')
    def start_task(**kwargs):
        # Фейковая задача - просто единая точка входа
        print('start')
    tasks.append(start_task())
# ==============================================================
    i = 1
    tasks.append([])
    for n in BASES:
    # Динамически генерируем параллельные задачи выгрузки для каждой базы с учетом количества каналов доступа
        @task(task_id=f'get_data_zbase_{n}')
        def get_data_zbase(i, **kwargs):
            # Выгрузка данных по одной управляющей компании с удаленного сервера
            ti = kwargs['ti']
            query = ""
            with open(f'{Variable.get("sql_script_path")}{os.sep}03_pump_search_all_zbases.sql', 'r') as file_sql:
                # Читаем SQL-скрипт из файла
                query = file_sql.read()
                fb_remote_connection_string = f"firebird+fdb://{Variable.get('FB_REMOTE_LOGIN')}:{Variable.get('FB_REMOTE_PASSWORD')}" \
                                              f"@{Variable.get('FB_REMOTE_HOST')}:{Variable.get('FB_REMOTE_PORT')}//{Variable.get('FB_REMOTE_PATH_TO_BASE')}/" \
                                              f"{Variable.get('FB_REMOTE_DBNAME')}{i}.fdb?charset=WIN1251&fb_library_name=/usr/lib/libfbclient.so"
                textual_sql = text(query)
                engine = create_engine(fb_remote_connection_string)
                metadata_obj = MetaData()
                metadata_obj.reflect(engine)
                with engine.connect() as connection:
                    metadata_obj.reflect(connection)
                    with connection.begin():
                        # Выполняем скрипт
                        data = connection.execute(textual_sql)
                        # помещаем данные в объект XCom для передачи на следующий этап обработки
                        ti.xcom_push(key=f'zbase_{i}', value=[dict(zip(columns_in,row)) for row in data])

        if i % CHANNELS == 0:  # Раскидываем задачи паралельно, занимая все возможные каналы
            tasks[-1].append(get_data_zbase(n))
            if n != BASES[-1]:
                tasks.append([])
        else:
            tasks[-1].append(get_data_zbase(n))
        i += 1
    while len(tasks[-1]) < CHANNELS:  # Дописываем последний блок параллельных зачад пустыми операторами
        tasks[-1].append(EmptyOperator(task_id=f'Empty{i}'))   # Параллельные блоки должны иметь одинаковое 
        i += 1                                                 # количество задач
# ==============================================================
    @task(task_id='clean_kvk_base', trigger_rule=TriggerRule.ALL_SUCCESS)
    def clean_kvk_base(**kwargs):
        # Очищаем рабочую таблицу, при условии, что все предыдущие задачи завершились успешно
        fb_local_connection_string = f"firebird+fdb://{Variable.get('FB_LOCAL_LOGIN')}:{Variable.get('FB_LOCAL_PASSWORD')}" \
                                     f"@{Variable.get('FB_LOCAL_HOST')}:{Variable.get('FB_LOCAL_PORT')}//{Variable.get('FB_LOCAL_PATH_TO_BASE')}/" \
                                     f"{Variable.get('FB_LOCAL_DBNAME')}.fdb?charset=WIN1251&fb_library_name=/usr/lib/libfbclient.so"
        engine = create_engine(fb_local_connection_string)
        metadata_obj = MetaData()
        metadata_obj.reflect(engine)

        with engine.connect() as connection:
            metadata_obj.reflect(connection)
            t = Table(LOCAL_RECEIVE_TABLE, metadata_obj)
            # Очистка данных
            with connection.begin():
                result = connection.execute(delete(t))
    tasks.append(clean_kvk_base())
# ==============================================================
    tasks.append([])
    i = 1
    # Динамически генерируем параллельные задачи загрузки для каждой базы
    for n in BASES:
        @task(task_id=f'push_data_to_kvk_{n}')
        def push_data_to_kvk(i, **kwargs):
            # Загружаем информацию по одной базе
            fb_local_connection_string = f"firebird+fdb://{Variable.get('FB_LOCAL_LOGIN')}:{Variable.get('FB_LOCAL_PASSWORD')}" \
                                         f"@{Variable.get('FB_LOCAL_HOST')}:{Variable.get('FB_LOCAL_PORT')}//{Variable.get('FB_LOCAL_PATH_TO_BASE')}/" \
                                         f"{Variable.get('FB_LOCAL_DBNAME')}.fdb?charset=WIN1251&fb_library_name=/usr/lib/libfbclient.so"
            engine = create_engine(fb_local_connection_string)
            metadata_obj = MetaData()
            metadata_obj.reflect(engine)
            with engine.connect() as connection:
                metadata_obj.reflect(connection)
                t = Table(LOCAL_RECEIVE_TABLE, metadata_obj,
                    Column('BASE', Integer),
                    Column('LS', Integer),
                    Column('FIO', String),
                    Column('ADDRESS', String),
                    Column('CLOSE_LS', Integer),
                    extend_existing=True
                )
                data = kwargs['ti'].xcom_pull(task_ids=[f'get_data_zbase_{i}'],key=f'zbase_{i}')[0]
                # Вставка данных
                with connection.begin():
                    result = connection.execute(insert(t), [dict(row, **{'BASE': i}) for row in data], )
        tasks[-1].append(push_data_to_kvk(n))
# ==============================================================
    @task(task_id='end', trigger_rule=TriggerRule.ALL_SUCCESS)
    @provide_session
    def end_task(session=None, **context):
        # Завершение работы + единая точка выхода
        print('end')
        # Чистим данные, которые были в XCom
        with settings.Session() as session:
            res = session.query(XCom).filter(XCom.dag_id == DAG_ID).delete()
            session.commit()
    tasks.append(end_task())
    # "Маршрут" процесса
#    start_task() >> tasks1 >> clean_kvk_base() >> tasks2 >> end_task()
    chain(*tasks)

dag = Pump_all_Zbases()
