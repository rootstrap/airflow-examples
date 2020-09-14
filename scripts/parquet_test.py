import s3parq as parq

bucket = 's3://medical-records/'
key = 'csv'
dataframe = pd.DataFrame()

pandas_dataframe = parq.fetch(  bucket=bucket,
                                key=key)