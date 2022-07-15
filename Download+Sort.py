import pandas as pd
import boto3

def download():
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket='wetterstationsbucket', Prefix='dataset/')
    objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'])
    latest_object = objects[-1]['Key']
    filename = latest_object[latest_object.rfind('/') + 1:]  # Remove path
    # Download in aktuellen ordner
    s3_client.download_file('wetterstationsbucket', latest_object, 'D1.csv')

    response = s3_client.list_objects_v2(Bucket='wetterstationsbucket', Prefix='d3/')
    objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'])
    latest_object = objects[-1]['Key']
    filename = latest_object[latest_object.rfind('/') + 1:]  # Remove path
    # Download in aktuellen ordner
    s3_client.download_file('wetterstationsbucket', latest_object, 'D3.csv')

    response = s3_client.list_objects_v2(Bucket='wetterstationsbucket', Prefix='d5/')
    objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'])
    latest_object = objects[-1]['Key']
    filename = latest_object[latest_object.rfind('/') + 1:]  # Remove path
    # Download in aktuellen ordner
    s3_client.download_file('wetterstationsbucket', latest_object, 'D5.csv')

    response = s3_client.list_objects_v2(Bucket='wetterstationsbucket', Prefix='d6/')
    objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'])
    latest_object = objects[-1]['Key']
    filename = latest_object[latest_object.rfind('/') + 1:]  # Remove path
    # Download in aktuellen ordner
    s3_client.download_file('wetterstationsbucket', latest_object, 'D6.csv')

    response = s3_client.list_objects_v2(Bucket='wetterstationsbucket', Prefix='d7/')
    objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'])
    latest_object = objects[-1]['Key']
    filename = latest_object[latest_object.rfind('/') + 1:]  # Remove path
    # Download in aktuellen ordner
    s3_client.download_file('wetterstationsbucket', latest_object, 'D7.csv')
    # -------------------------------------------------------------------------------------------------------------
download()
d1 = pd.read_csv('D1.csv')
#Sortieren der CSV-Datei nach Datum
d1['date'] = pd.to_datetime(d1.timestamp, infer_datetime_format=True)
d1.sort_values(by='date', ascending=True, inplace=True)
d1_neu = d1
d1_neu.to_csv('D1_sortiert.csv')

d2 = pd.read_csv('D3.csv')
#Sortieren der CSV-Datei nach Datum
d2['date'] = pd.to_datetime(d2.timestamp, infer_datetime_format=True)
d2.sort_values(by='date', ascending=True, inplace=True)
d2_neu = d2
d2_neu.to_csv('D3_sortiert.csv')

d3 = pd.read_csv('D5.csv')
#Sortieren der CSV-Datei nach Datum
d3['date'] = pd.to_datetime(d3.timestamp, infer_datetime_format=True)
d3.sort_values(by='date', ascending=True, inplace=True)
d3_neu = d3
d3_neu.to_csv('D5_sortiert.csv')

d4 = pd.read_csv('D6.csv')
#Sortieren der CSV-Datei nach Datum
d4['date'] = pd.to_datetime(d4.timestamp, infer_datetime_format=True)
d4.sort_values(by='date', ascending=True, inplace=True)
d4_neu = d4
d4_neu.to_csv('D6_sortiert.csv')

d5 = pd.read_csv('D7.csv')
#Sortieren der CSV-Datei nach Datum
d5['date'] = pd.to_datetime(d2.timestamp, infer_datetime_format=True)
d5.sort_values(by='date', ascending=True, inplace=True)
d5_neu = d5
d5_neu.to_csv('D7_sortiert.csv')