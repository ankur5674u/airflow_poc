# airflow related
from airflow import models
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# other packages
from datetime import datetime, timedelta
import uuid
from pathlib import Path


# Project related


class EdwPandasWrapper(object):
    @staticmethod
    def read_csv_file_in_data_frame(local_path, dtype=str, encoding='UTF-8', sep='|', index_col=False):
        import pandas as pd
        return pd.read_csv(local_path, encoding=encoding, dtype=dtype, sep=sep, index_col=index_col)

    @staticmethod
    def write_csv_file_from_data_frame(df, file_path, encoding='UTF-8', sep='|', compression=None, index=False):
        df.to_csv(file_path, encoding=encoding, sep=sep, compression=compression, index=index)


def load_and_dqm_pipeline(**context):
    (file_name, batch_id) = (context['file_name'], context['batch_id'])
    file_path = (Path(__file__).parent / "../landing_files" / file_name).resolve()
    file_df = EdwPandasWrapper.read_csv_file_in_data_frame(file_path, sep=',')
    print(f"File column_list :{','.join(list(map(lambda x: x.upper(), file_df.columns.to_list())))}")
    print(f"File Name :{file_name}")
    print(f"Total Record :{file_df.shape[0]}")
    print(f"Batch ID:{batch_id}")
    task_instance = context['task_instance']
    task_instance.xcom_push('batch_id', batch_id)
    task_instance.xcom_push('file_name', file_name)
    return True


def manipulate_dqm_pipeline(**context):
    batch_id = context['task_instance'].xcom_pull(task_ids='load_and_dqm_pipeline', key='batch_id')
    file_name = context['task_instance'].xcom_pull(task_ids='load_and_dqm_pipeline', key='file_name')
    file_path = (Path(__file__).parent / "../landing_files" / file_name).resolve()
    file_df = EdwPandasWrapper.read_csv_file_in_data_frame(file_path, sep=',')
    file_df['batch_id'] = batch_id
    out_file_path = (Path(__file__).parent / "../output_files" / "_".join([file_name.split('_')[0],
                                                                          datetime.utcnow().strftime(
                                                                              '%Y-%m-%d %H:%M:%S.%f')])).resolve()
    EdwPandasWrapper.write_csv_file_from_data_frame(file_df,out_file_path)
    return True


default_dag_args = dict(
    start_date=datetime(2020, 1, 4, 5),
    email_on_failure=False,
    email_on_retry=False,
    project_id='edw_etl_demo',
)


def generate_unique_task_id():
    return uuid.uuid5(uuid.NAMESPACE_DNS, '_'.join(
        ['load_and_dqm_pipeline', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')])).__str__()


with models.DAG(dag_id='edw_etl', schedule_interval='5 4 * * *',
                catchup=True, default_args=default_dag_args) as dag:
    load_input_data_operator = PythonOperator(
        task_id='load_and_dqm_pipeline',
        provide_context=True,
        python_callable=load_and_dqm_pipeline,
        op_kwargs=dict(file_name='carriage_service_04012019_04012020.csv', batch_id=generate_unique_task_id()),
        dag=dag)

    manipulate_dqm_operator = PythonOperator(
        task_id='manipulate_dqm_pipeline',
        provide_context=True,
        python_callable=manipulate_dqm_pipeline,
        # op_kwargs=dict(file_name='carriage_service_04012019_04012020.csv', batch_id=generate_unique_task_id()),
        dag=dag)
    load_input_data_operator >> manipulate_dqm_operator

