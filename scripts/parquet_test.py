from s3parq import S3Parq

bucket = 's3://medical-records/'
key = 'csv'

df = pd.DataFrame()
parq = S3Parq(bucket=bucket,dataset=df)
retrieved_dataframe = parq.fetch()

