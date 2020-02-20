class EdwPandasWrapper(object):
    @staticmethod
    def read_csv_file_in_data_frame(local_path, dtype=str, encoding='UTF-8', sep='|', index_col=False):
        import pandas as pd
        return pd.read_csv(local_path, encoding=encoding, dtype=dtype, sep=sep, index_col=index_col)

    @staticmethod
    def write_csv_file_from_data_frame(df, file_path, encoding='UTF-8', sep='|', compression=None, index=False):
        df.to_csv(file_path, encoding=encoding, sep=sep, compression=compression, index=index)
