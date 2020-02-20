from pathlib import Path

from dags.utility.pandas_wrapper import EdwPandasWrapper


def load_and_dqm_pipeline(**context):
    file_path = (Path(__file__).parent / "../landing_files" / context['file_name']).resolve()
    file_df = EdwPandasWrapper.read_csv_file_in_data_frame(file_path, sep=',')
    print(context)
    return file_df