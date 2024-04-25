import datetime
import os
import requests
import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine, MetaData, insert, Table, Column, Integer, String
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import xlsxwriter

from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import smtplib

DAG_ID = "01.Monthly_Statistic"

receivers = ['mordanov.da@rkc43.ru'
#, 'dudnikov.an@rkc43.ru'
, 'mikrukova.ts@rkc43.ru'
, 'hodyrevrv@vdkanal.ru'
]


default_args = {
    'owner': 'MordanovDA',
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2024, month=4, day=25).in_timezone('Europe/Moscow'),
    'email': ['mordanov.da@rkc43.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5)
}



@dag(
    dag_id=DAG_ID,
    schedule="0 2 4-15 * *",
#    schedule=None,
    start_date=pendulum.datetime(2024, 3, 26, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    description="Собираем ежемесячную статистику",
    default_args=default_args,
)

def Monthly_Statistic():
    operator_start = EmptyOperator(task_id='start')

    @task.branch(task_id='check_period')
    def check_period():
        # Проверка, когда запускался DAG (сравниваемм обработанные периоды)
        oracle_connection_string = f"oracle+cx_oracle://{Variable.get('ORA_LOGIN')}:{Variable.get('ORA_PASSWD')}" \
                                   f"@{Variable.get('ORA_HOST')}:{Variable.get('ORA_PORT')}/{Variable.get('ORA_DATABASE')}"
        engine_ora = create_engine(oracle_connection_string)
        df_ora = pd.read_sql("select bl_bill_id from bl_bill_periods where bl_bill_id=" \
                             "(select max(bl_bill_id) from bl_bill_periods where period_status='O')", engine_ora)
        period_ora = int(df_ora.to_string(index=False, header=False))

        pg_connection_string = f"postgresql://{Variable.get('PG_LOGIN')}:{Variable.get('PG_PASSWD')}" \
                               f"@{Variable.get('PG_HOST')}:{Variable.get('PG_PORT')}/{Variable.get('PG_DB')}"
        engine_pg = create_engine(pg_connection_string)
        df_pg = pd.read_sql(f"select coalesce(max(exec_period),0) as exec_period from dag_run where dag_name='{DAG_ID}'", engine_pg)
        period_pg = int(df_pg.to_string(index=False, header=False))

        if period_ora != period_pg:
            # в этом периоде обработки еще не было
            metadata_obj = MetaData()
            metadata_obj.reflect(engine_pg)
            with engine_pg.connect() as connection:
                metadata_obj.reflect(connection)
                t = Table('dag_run', metadata_obj,
                    Column('id', Integer),
                    Column('dag_name', String),
                    Column('exec_period', Integer),
                    extend_existing=True
                )
                with connection.begin():
                    # фиксируем событие обработки
                    result = connection.execute(insert(t), {'dag_name':DAG_ID, 'exec_period':period_ora})
            return 'check_period_true'
        else:
            return 'check_period_false'

    check_period = check_period.override(task_id='check_period')()

    check_period_true = EmptyOperator(task_id='check_period_true')   # фейковые операторы - для "маршрута" ветвления
    check_period_false = EmptyOperator(task_id='check_period_false')

    @task(task_id='get_data_from_oracle')
    def get_data_from_oracle(**kwargs):
        # достаем информацию из Oracle
        ti = kwargs['ti']
        query = ""
        # Скрипт - достаем из общей папки
        with open(f'{Variable.get("sql_script_path")}{os.sep}01.monthly_stats.sql', 'r') as file_sql:
            query = file_sql.read()
        oracle_connection_string = f"oracle+cx_oracle://{Variable.get('ORA_LOGIN')}:{Variable.get('ORA_PASSWD')}" \
                                   f"@{Variable.get('ORA_HOST')}:{Variable.get('ORA_PORT')}/{Variable.get('ORA_DATABASE')}"
        engine = create_engine(oracle_connection_string)
        # Выполняем SQL
        df = pd.read_sql(query, engine)
        tmp_file = f"{Variable.get('temp_path')}{os.sep}Статистика.xlsx"

        # Пишем результат в xlsx файл
        with pd.ExcelWriter(tmp_file, engine='xlsxwriter') as wb:
            df.to_excel(wb, sheet_name='Лист1', index=False)
            sheet = wb.sheets['Лист1']
            sheet.set_column('A:A',50)  # ширина полей
            sheet.set_column('B:B',17)

        # передаем имя файла следующей задаче
        ti.xcom_push(key='xlsx_file', value=tmp_file)

    @task(task_id='xlsx_to_email')
    def xlsx_to_email(**kwargs):
        # готовим электронное письмо и отправляем его получателям
        tmp_file = str(kwargs['ti'].xcom_pull(task_ids=['get_data_from_oracle'],key='xlsx_file')[0])
        oracle_connection_string = f"oracle+cx_oracle://{Variable.get('ORA_LOGIN')}:{Variable.get('ORA_PASSWD')}" \
                                   f"@{Variable.get('ORA_HOST')}:{Variable.get('ORA_PORT')}/{Variable.get('ORA_DATABASE')}"
        engine = create_engine(oracle_connection_string)
        df = pd.read_sql("select bill_name from bl_bill_periods where bl_bill_id=" \
                         "(select max(bl_bill_id) from bl_bill_periods where period_status='C')", engine)
        period = df.to_string(index=False, header=False)
        subject = 'Статистика по биллинговой базе за ' + period
        email_text = f'<html>' \
                     f'<h3>' \
                     f'Добрый день!' \
                     f'</h3>' \
                     f'<p>Высылаю статистику по биллинговой базе за {subject}' \
                     f'</p>' \
                     f'<p></p>' \
                     f'<p>--<br>' \
                     f'---<br>' \
                     f'С уважением,' \
                     f'Робот ООО РКЦ' \
                     f'</p>' \
                     f'</html>'
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = formataddr((Variable.get('email_from_name'), Variable.get('email_from_address')))
        msg['To'] = ", ".join(receivers)

        with open(tmp_file, 'rb') as file:  # вкладываем вложение
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-disposition', 'attachment', filename=('utf-8', '', os.path.basename(tmp_file)))
            msg.attach(part)
        msg.attach(MIMEText(email_text.encode('utf-8'), 'html', 'UTF-8'))

        with smtplib.SMTP(Variable.get('email_server'), Variable.get('email_server_port')) as server:
            server.sendmail(Variable.get('email_from_address'), receivers, msg.as_string())

        os.remove(tmp_file)


    operator_end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # "Маршрут" задачи
    operator_start >> check_period
    check_period >> check_period_true >> get_data_from_oracle() >> xlsx_to_email() >> operator_end
    check_period >> check_period_false >> operator_end

dag = Monthly_Statistic()
