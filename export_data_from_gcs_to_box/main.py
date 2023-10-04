'''
Written by:    Jake Peters
Written:       July 2022
Last Modified: July 2022
Description:   This Cloud Function moves files from Google Cloud Storage to Box
               based on filename tags. It dynamically identifies Box folders
               using tags in filenames, such as '_fileid_'.

Usage:
1. Trigger this function when a new file is created in the specified
   Google Cloud Storage bucket.
2. The function checks for the "_fileid_" tag in the filename.
3. If found, the file is moved to the corresponding Box folder on Box.com.
4. If a file with the same "_fileid_" exists in the destination Box folder, its
   contents are updated with the new file's contents.

Example Filenames:
- 'HealthPartners_deidentified_recruitment_data_fileid_217146133744.csv'

Pre-requisites:
- Google Cloud Storage
- Box.com account
- Google Secret Manager with Box token configured
'''

import re
import io
import json
from google.cloud import secretmanager, storage
from boxsdk import JWTAuth, Client

# Define constants
PROJECT_ID = 155089172944
SECRET_ID = "boxtoken"

def file_to_be_exported(file_name):
    '''Check for "_boxfolder_" to prevent accidental exports of files dropped in this bucket.'''
    return "_boxfolder_" in file_name

def download_file_contents(bucket_name, file_name):
    '''Download file contents from Google Cloud Storage.'''
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    return blob.download_as_bytes()

def extract_box_folder_and_file_ids(file_name):
    '''Extract Box folder and file IDs from the filename.'''
    pattern1 = r'_boxfolder_(\d{12})'
    pattern2 = r'_fileid_(\d{13})'
    box_folder_match = re.search(pattern1, file_name)
    box_file_match = re.search(pattern2, file_name)
    if box_folder_match and box_file_match:
        return box_folder_match.group(1), box_file_match.group(1)
    return None, None

def update_box_file(box_client, box_folder_id, box_file_id, file_contents, file_name):
    '''Update a Box file with new contents.'''
    stream = io.BytesIO(file_contents)
    updated_file = box_client.file(box_file_id).update_contents_with_stream(stream)
    print(f'File "{file_name}" has been updated')

def get_box_token(version_id="latest"):
    '''Get Box token from Google Secret Manager.'''
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode('UTF-8')

def get_box_client(box_token):
    '''Authenticate to Box.com'''
    service_account_auth = JWTAuth(
        client_id=box_token['boxAppSettings']['clientID'],
        client_secret=box_token['boxAppSettings']['clientSecret'],
        enterprise_id=box_token['enterpriseID'],
        jwt_key_id=box_token['boxAppSettings']['appAuth']['publicKeyID'],
        rsa_private_key_data=box_token['boxAppSettings']['appAuth']['privateKey'],
        rsa_private_key_passphrase=box_token['boxAppSettings']['appAuth']['passphrase']
    )
    access_token = service_account_auth.authenticate_instance()
    service_account_client = Client(service_account_auth)
    return service_account_client

def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file_object = event
    print(f"File object: {file_object}")
    print(f"Processing file: {file_object['name']}.")

    if file_to_be_exported(file_object['name']):
        box_token = json.loads(get_box_token())
        box_client = get_box_client(box_token)
        box_folder_id, box_file_id = extract_box_folder_and_file_ids(file_object['name'])

        if box_folder_id and box_file_id:
            file_contents = download_file_contents(file_object['bucket'], file_object['name'])
            update_box_file(box_client, box_folder_id, box_file_id, file_contents, file_object['name'])
        else:
            print("Unable to extract Box folder and file IDs from the filename.")