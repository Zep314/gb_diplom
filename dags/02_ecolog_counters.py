import datetime
import os
import requests
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import smtplib

receivers = ['mordanovda@vdkanal.ru'
, 'gaponenkoaa@vdkanal.ru'
, 'karavaevaev@vdkanal.ru'
]


default_args = {
    'owner': 'MordanovDA',
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2023, month=11, day=24).in_timezone('Europe/Moscow'),
    'email': ['mordanov.da@rkc43.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5)
}

@dag(
    dag_id="02.Ecolog_Counters",
#    schedule="0 5 10 * *",
    schedule=None,
    start_date=pendulum.datetime(2024, 3, 26, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    description="Выгружаем показания счетчиков и нормативы ливневых стоков для экологов",
    default_args=default_args,
)

def ecolog_counters():

    @task(task_id='get_data_from_oracle')
    def get_data_from_oracle(**kwargs):
        ti = kwargs['ti']
        query = ""
        # Загружаем SQL скрипт из файла
        with open(f'{Variable.get("sql_script_path")}{os.sep}02.ecolog_counters.sql', 'r') as file_sql:
            query = file_sql.read()
        oracle_connection_string = f"oracle+cx_oracle://{Variable.get('ORA_LOGIN')}:{Variable.get('ORA_PASSWD')}" \
                                   f"@{Variable.get('ORA_HOST')}:{Variable.get('ORA_PORT')}/{Variable.get('ORA_DATABASE')}"
        engine = create_engine(oracle_connection_string)
        # Выполнение скрипта
        df = pd.read_sql(query, engine)
        tmp_file = f"{Variable.get('temp_path')}{os.sep}Счетчики_для_экологов.xlsx"

        with pd.ExcelWriter(tmp_file, engine='xlsxwriter') as wb:
            df.to_excel(wb, sheet_name='Лист1', index=False)
            # Настраиваем ширину полей в Excel-файле
            sheet = wb.sheets['Лист1']
            sheet.set_column('A:A',19)
            sheet.set_column('B:B',33)
            sheet.set_column('C:C',25.43)
            sheet.set_column('D:D',44)
            sheet.set_column('E:E',24)
            sheet.set_column('F:F',17)
            sheet.set_column('G:H',28)
            sheet.set_column('I:I',20)
            sheet.set_column('J:J',7)
            sheet.set_column('K:K',10)
            sheet.set_column('L:L',22.7)
            sheet.set_column('M:M',19.71)

        # Передача данных между задачами
        ti.xcom_push(key='xlsx_file', value=tmp_file)

    @task(task_id='xlsx_to_email')
    def xlsx_to_email(**kwargs):
        # Получаем данные от предыдущей задачи
        tmp_file = str(kwargs['ti'].xcom_pull(task_ids=['get_data_from_oracle'],key='xlsx_file')[0])
        oracle_connection_string = f"oracle+cx_oracle://{Variable.get('ORA_LOGIN')}:{Variable.get('ORA_PASSWD')}" \
                                   f"@{Variable.get('ORA_HOST')}:{Variable.get('ORA_PORT')}/{Variable.get('ORA_DATABASE')}"
        engine = create_engine(oracle_connection_string)
        df = pd.read_sql("select bill_name from bl_bill_periods where bl_bill_id=" \
                         "(select max(bl_bill_id) from bl_bill_periods where period_status='O')", engine)
        period = df.to_string(index=False, header=False)
        # Готовим данные для электронного письма
        subject = 'Счетчики для экологов за ' + period
        email_text = f'<html>' \
                     f'<h3>' \
                     f'Добрый день!' \
                     f'</h3>' \
                     f'<p>Высылаю информацию о показаниях счетчиков для экологов по открытому периоду: {period}' \
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
        # Отправка письма
        with smtplib.SMTP(Variable.get('email_server'), Variable.get('email_server_port')) as server:
            server.sendmail(Variable.get('email_from_address'), receivers, msg.as_string())
        # Чистим за собой ненужные данные
        os.remove(tmp_file)

    get_data_from_oracle() >> xlsx_to_email()

dag = ecolog_counters()
