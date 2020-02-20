import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import yaml
default_args = {'owner': 'airflow','start_date':'2020-01-01','retries':0}

DM_CONFIG_PATH = '/home/DOM.INCENTIUS.COM/ankur.singh/workspace/incentivate_dm_config/INCENTIVATE/INDIA/'


def mock_func(*args,**kwargs):
	print(args[0])
	print(kwargs['conf'])

def get_python_callable(step_config):
	return mock_func


for f in os.listdir(os.path.join(DM_CONFIG_PATH,'jobs')):
	if f.lower().endswith('.yaml'):
		job_config = yaml.safe_load(open(os.path.join(DM_CONFIG_PATH,'jobs',f),'r'))
		with DAG(job_config['id'],default_args=default_args) as dag:
			steps = dict()
			for step in job_config['steps']:
				steps[step['step_id']] = PythonOperator(task_id=step['step_id'],
														python_callable=get_python_callable(step),
														op_args=[step],
														provide_context=True,
														schedule_interval=None
														)
				if 'step_inputs' in step:
					if isinstance(step['step_inputs'],list):
						for input in step['step_inputs']:
							steps[step['step_id']].set_upstream(steps[input])
					else:
						steps[step['step_id']].set_upstream(steps[step['step_inputs']])

			globals()[job_config['id']] = dag
