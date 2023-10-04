'''
Written by:    Jake Peters
Written:       July 2022
Last Modified: July 2022
Description:   This cloud function uses a Custom box app service account
               to move a file to a specified Box folder when the file has 
               been put into a specified Cloud Storage bucket.
               The Box folder admin must invite the Box app service account
               as a user for the folder. 

               When a file is created in the bucket 'deidentified_site_recruitment_data'
               that containes the tag '_boxfolder_' in it's filename, it is shunted to 
               the approprate folder in box. 
               
               For example, 'HealthPartners_deidentified_recruitment_data_boxfolder_217146133744.csv'
               would be written to the Box folder indicated by '217146133744'.
'''

from google.cloud import secretmanager, storage
from boxsdk import JWTAuth, Client
import json
import io
import re

PROJECT_ID = 155089172944

def gcs2box_on_file_creation_event(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    fileObject = event
    print(f"File object: {fileObject}")
    print(f"Processing file: {fileObject['name']}.")

    ## get the contents of the newly created file on GCP...
    storageClient = storage.Client()
    bucket  = storageClient.bucket(fileObject['bucket'])
    blob = bucket.blob(fileObject['name'])
    contents = blob.download_as_bytes()
    print(f'length of bites = {len(contents)}')

    ## get the service account from Box and create a client...
    boxToken = json.loads( get_box_token() )
    boxClient = get_box_client(boxToken)

    ## Move file to Box
    # Documentation: https://github.com/box/box-python-sdk/blob/main/docs/usage/files.md#upload-a-file
    # NOTE: box requires either a Stream or a file.  Since we do not have
    #       files on GCS, convert the byte-array (contents) into a stream
    #       and upload it to box.    
    stream = io.BytesIO(contents)
    # Check for "_boxfolder_" tag in filename before exporting
    if fileToBeExported(fileObject['name']):

        # Get box folder id
        pattern1 = r'_boxfolder_(\d{12})'
        matches = re.findall(pattern1, fileObject['name'])
        box_folder_id = matches[0]

        # Get box file id
        pattern2 = r'_fileid_(\d{13})'
        matches = re.findall(pattern2, fileObject['name'])
        box_file_id = matches[0]

        # Remove the "_boxfolder_xxxxxxxxxxx" tag & "_fileid_xxxxxxx" from the filename before exporting to box
        file_name = re.sub(pattern1, "", fileObject['name']) # remove "_boxfolder_xxxxxxxxxxx"
        file_name = re.sub(pattern2, "", file_name) # remove "_fileid_xxxxxxx"

        #new_file = boxClient.folder(box_folder_id).upload_stream(stream, fileObject['name'])

        # Update file
        updated_file = boxClient.file(box_file_id).update_contents_with_stream(stream)
        print(f'File "{updated_file.name}" has been updated')

def fileToBeExported(file_name):
    '''Check for "_boxfolder_" to prevent accidental exports of files dropped in this bucket.'''
    if "_boxfolder_" in file_name:
        return True
    else:
        return False

def stoken_callback(token, arg2):
  '''This function is used as a callback by the in Box's JWTAuth object. 
  It takes a token as an argument and returns it, so it basically does 
  nothing, but it is required.'''
  return token

def get_box_token(version_id="latest"):
    '''Get Secrets
    Documentation: https://github.com/box/box-python-sdk/blob/main/docs/usage/files.md#upload-a-file
    '''

    secret_id = "boxtoken" # This is the name of the Cloud Secret set up for this purpose by Daniel Russ
    #PROJECT_ID = 1061430463455 # This is the project_id for the `nih-nci-dceg-connect-dev` environment
    #TODO pass project as argument. Define at top or get from environ.
    # The PROJECT_ID must be set as an environment variable in the cloud function configuration settings.
    # PROJECT_ID = os.getenv('PROJECT_ID')

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return the decoded payload.
    return response.payload.data.decode('UTF-8')

def get_box_client(boxToken):
    '''Authenticate to Box.com
    Documentation: https://github.com/box/box-python-sdk/blob/main/docs/usage/authentication.md
    '''
    service_account_auth = JWTAuth(
        client_id = boxToken['boxAppSettings']['clientID'],
        client_secret = boxToken['boxAppSettings']['clientSecret'],
        enterprise_id = boxToken['enterpriseID'],
        jwt_key_id = boxToken['boxAppSettings']['appAuth']['publicKeyID'],
        rsa_private_key_data = boxToken['boxAppSettings']['appAuth']['privateKey'],
        rsa_private_key_passphrase = boxToken['boxAppSettings']['appAuth']['passphrase'],
        store_tokens=stoken_callback)
    # print(' ==================== THE SERVICE ACCOUNT AUTH IS ', service_account_auth)
    access_token = service_account_auth.authenticate_instance()
    service_account_client = Client(service_account_auth)
    return service_account_client