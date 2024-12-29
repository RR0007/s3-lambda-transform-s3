import pandas as pd
import boto3
import json
def lambda_handler(event, context):
    s3=boto3.client('s3')
    input_bucket=event['Records'][0]['s3']['bucket']['name']
    input_keys=event['Records'][0]['s3']['objetc']['key']

    obj=s3.get_object(Bucket=input_bucket,Key=input_keys)
    body=obj['Body'].read().decode('utf-8')
    json_dic=body.split('\r\n')
    
    data=[json.load(line) for line in json_dic]
    df=pd.DataFrame(data)

    df_filter=df[df['status']=='delivered']
    print("Filtered DataFrame: ", df_filter)

    filtered_json = df_filter.to_json(orient='records', lines=True)


    s3.put_object(Bucket="doordash-target-zn-demo",Key="doordash-target-zn-demo/filter.json",Body=filtered_json)


    return {
            'statusCode': 200,
            'body': json.dumps('Data processing and upload successful!')
        }

    
