import os
import pandas as pd
import boto3

class S3:
    def __init__(self):
        # Initiate Values
        self.bucket = 'onlyvectors'
        self.s3 = boto3.client(
            's3',
            region_name = 'ap-southeast-1',
            aws_access_key_id = os.environ['LAMBDA_AWS_ACCESS_KEY'],
            aws_secret_access_key = os.environ['LAMBDA_AWS_SECRET_ACCESS_KEY']
        )

    def is_within_size_limit(self, key, threshold=200000000):
        # Get Head
        head = self.s3.head_object(Bucket=self.bucket, Key=key)

        # File is within threshold
        if head['ContentLength'] < threshold:
            return True
        else:
            print(f'File too big: {head["ContentLength"]}')
            return False

    def get_file_as_dataframe(self, filename):
        # Set Values
        filetype = filename.split('.')[-1]
        key = f"data/files/{filename}"
        print(self.bucket)
        print(key)

        if filetype in ['csv', 'xlsx', 'json']:
            if self.is_within_size_limit(key):
                # Get Object
                response = self.s3.get_object(Bucket=self.bucket, Key=key)

                if filetype == 'csv':
                    # Get CSV
                    df = pd.read_csv(response['Body'], encoding="utf-8")

                elif filetype == 'xlsx':
                    # Get Excel File
                    df = pd.read_excel(response['Body'].read(), header=0)

                elif filetype == 'json':
                    df = pd.read_json(response['Body'], encoding="utf-8")
                else:
                    return False

                if 'Unnamed: 0' in df.columns:
                    df = df.drop(columns=['Unnamed: 0'])
                return df.fillna('')
            else:
                print('File Exceeds Limit')
                return False
        else:
            print('Write Format')
            return False