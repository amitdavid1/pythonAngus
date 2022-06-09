import json
import boto3
import pandas as pd
import paramiko
from sqlalchemy import create_engine

secret_name = 'preprod'
dbname = "preprod"
region_name = "us-east-2"
schema_name = 'trm_data'
profile_name = 'dev'


Kilroy_table =pd.DataFrame()
Zeller_table =pd.DataFrame()

def get_secret():
    # Create a Secrets Manager client
    session = boto3.session.Session(profile_name=profile_name)
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    return json.loads(get_secret_value_response['SecretString'])

def db_string():
    _secret = get_secret()
    return f"""postgresql://{_secret["username"]}:{_secret["password"]}@{_secret["host"]}:{_secret["port"]}/{dbname}"""

def get_db():
    return create_engine(
        db_string())

# Get CSV file from FileZila
def get_csv_file_from_sftp():
    host = 'sftp1.angusanywhere.com'
    port = 55055
    transport = paramiko.Transport((host, port))
    password = 'F2XZeo5z8uu6pAW2ZTRCKN3aipEV85'
    username = 'okapi'
    transport.connect(username=username, password=password)
    # Connecting!
    sftp = paramiko.SFTPClient.from_transport(transport)
    # Download
    filepath_Kilroy = '/files/Kilroy/TR/TRIncrement_KilroyRealty.csv'
    localpath_Kilroy = './Downloaded_Kilroy_file.csv'
    sftp.get(filepath_Kilroy, localpath_Kilroy)
    filepath_Zeller = '/files/Zeller/TR/TRIncrement_Zeller.csv'
    localpath_Zeller = './Downloaded_Zeller_file.csv'
    sftp.get(filepath_Zeller, localpath_Zeller)
    sftp.close()
    transport.close()

def convert_format_kilroy():
    df = pd.read_csv('./Downloaded_Kilroy_file.csv', encoding='unicode_escape')
    df_Kilroy = pd.DataFrame(df)
    df_Kilroy[['StatusDate','DateDue', 'DateOpened', 'DateDispatched', 'DateAccepted', 'DateWorkStarted', 'DateEstimateGenerated',
                'DateEstimateApproved', 'DateCompleted', 'DateClosed', 'DateVerified', 'DateInvoiced', 'DateEscalated1',
                'DateEscalated2', 'DateEscalated3',
                'DateDelayed']] = df_Kilroy[['StatusDate', 'DateDue', 'DateOpened', 'DateDispatched', 'DateAccepted',
                                        'DateWorkStarted', 'DateEstimateGenerated', 'DateEstimateApproved',
                                        'DateCompleted', 'DateClosed', 'DateVerified', 'DateInvoiced',
                                        'DateEscalated1', 'DateEscalated2', 'DateEscalated3', 'DateDelayed']].apply(pd.to_datetime)
    df_Kilroy.to_sql('kilroy_wo_manual', get_db(), schema='trm_data', if_exists='replace', index=False)
    print('trm_data.kilroy_wo_manual has been created!')

def convert_format_zeller():
    df= pd.read_csv('./Downloaded_Zeller_file.csv', encoding='unicode_escape')
    df_Zeller = pd.DataFrame(df)
    df_Zeller[['StatusDate','DateDue', 'DateOpened', 'DateDispatched', 'DateAccepted', 'DateWorkStarted', 'DateEstimateGenerated',
                'DateEstimateApproved', 'DateCompleted', 'DateClosed', 'DateVerified', 'DateInvoiced', 'DateEscalated1',
                'DateEscalated2', 'DateEscalated3',
                'DateDelayed']] = df_Zeller[['StatusDate', 'DateDue', 'DateOpened', 'DateDispatched', 'DateAccepted',
                                        'DateWorkStarted', 'DateEstimateGenerated', 'DateEstimateApproved',
                                        'DateCompleted', 'DateClosed', 'DateVerified', 'DateInvoiced',
                                        'DateEscalated1', 'DateEscalated2', 'DateEscalated3', 'DateDelayed']].apply(pd.to_datetime)
    df_Zeller.to_sql('zeller_wo_manual', get_db(),schema = 'trm_data', if_exists='replace', index=False)
    print('trm_data.zeller_wo_manual has been created!')


if __name__ == '__main__':

    get_csv_file_from_sftp()
    convert_format_kilroy()
    convert_format_zeller()