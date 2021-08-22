
import requests
import os
import json
from azure.storage.blob import BlobClient
from time import sleep

KBC_API_Assignment_token = 'xxx'

# Source filename (including path)
filePath = "D:\\Dokumenty\\15 - IT\\Keboola\\kbc_api_assignment.csv"
fileName = "kbc_api_assignment_2.csv"

# Target Storage Bucket (assumed to exist)
bucketName = 'in.c-KBC_API_Assignment'

# Target Storage Table (assumed NOT to exist)
tableName = 'KBC_API_Assignment_2'

# Create a new file in Storage
response = requests.post(
    'https://connection.north-europe.azure.keboola.com/v2/storage/files/prepare', #?federationToken=1',
    data={
        'name': fileName,
        'sizeBytes': os.stat(filePath).st_size
    },
    headers={'X-StorageApi-Token': KBC_API_Assignment_token}
)
parsed = json.loads(response.content.decode('utf-8'))
# print(json.dumps(parsed, indent=4))

# Get SAS token
fileId = parsed["id"]
account = parsed["absUploadParams"]["accountName"]
sas_token = parsed["absUploadParams"]["absCredentials"]["SASConnectionString"]
sas_token_2 = sas_token[(sas_token.find("SharedAccessSignature=") + len("SharedAccessSignature=")):]
blob_url = parsed["url"]
blob_container = parsed["absUploadParams"]["container"]
blob_name = parsed["absUploadParams"]["blobName"]

blob = BlobClient(account_url=blob_url, credential=sas_token_2, container_name=blob_container, blob_name=blob_name)
with open(filePath, "rb") as data:
    blob.upload_blob(data=data, overwrite=True)


response = requests.post(
    f'https://connection.north-europe.azure.keboola.com/v2/storage/buckets/{bucketName}/tables-async',
    data={'name': tableName, 'dataFileId': fileId, 'delimiter': ',', 'enclosure': '"'},
    headers={'X-StorageApi-Token': KBC_API_Assignment_token},
)

parsed = json.loads(response.content.decode('utf-8'))
if (parsed['status'] == 'error'):
    print(parsed['error'])
    exit(2)

status = parsed['status']
while (status == 'waiting') or (status == 'processing'):
    print('\nWaiting for import to finish')
    response = requests.get(parsed['url'], headers={'X-StorageApi-Token': KBC_API_Assignment_token})
    jobParsed = json.loads(response.content.decode('utf-8'))
    status = jobParsed['status']
    sleep(3)

if (jobParsed['status'] == 'error'):
    print(jobParsed['error']['message'])
    exit(2)

