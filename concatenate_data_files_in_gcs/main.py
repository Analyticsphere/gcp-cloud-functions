'''
    Written by:    Jake Peters
    Written:       Oct 2022
    Last Modified: Oct 2022
    Description:   This script is designed to be used as a GCP Cloud Function.
                   It is triggered when a new file object is finalized in the GCS bucket.
                   When a new "_HEADER_" file is detected, the function knows that a series of "_BODY_" files
                   have also been exported from BigQuery and need to be concatenated into a single file.
                   The resultant file will be exported to Box.com by a separate GCP Cloud Function.
'''

import re
from google.cloud import storage
import os
import subprocess

def main(event, context=None):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file_object = event
    print(f"File object: {file_object}")
    print(f"Processing file: {file_object['name']}.")

    if is_header_file(file_object['name']):
        print(f"{file_object['name']} is a header file. Triggering concatenation!")
        
        # Construct full path to file,
        # e.g., "gs://deidentified_site_recruitment_data_prod/Sanford/tmp/...
        #       Sanford_deidentified_recruitment_data...
        #       _box_folder_227964841688_file_id_1318220507784_HEADER_000000000000.csv"
        full_path = file_object['bucket'] + "/" + file_object['name']
        print(f"full_path = {full_path}")
        
        # Separate the folder path and filename
        # e.g., folder_path = gs://deidentified_site_recruitment_data_prod/Sanford/tmp
        folder_path, file_name = os.path.split(full_path)
        print(f"folder_path = {folder_path}")
        
        # Get just the site_name/tmp portion of the folder_path, e.g., "Sanford/tmp"
        parts = folder_path.split('/')
        site_tmp_folder = os.path.join(parts[-2], parts[-1])
        print(f"site_tmp_folder = {site_tmp_folder}")
        
        # Generate the output name 
        output_file_name = generate_output_file_path(full_path)
        print(f"output_filename = {output_file_name}")
    
        # Get a list of blobs from the <site>/tmp folder in the <bucket>
        bucket_name = file_object['bucket']
        print(f"bucket_name = {bucket_name}")
        blob_list = list_bucket_blobs(bucket_name, site_tmp_folder)
        
        # Concatenate the blobs, ensuring that the header file is at the top
        print(f'Attempting to concatenate the followingblobs: \n {[blob.name for blob in blob_list]}')
        concatenate_blobs(blob_list, output_file_name)
        print(f'Successfully concatenated to {output_file_name}')
        
        # Delete the files from the tmp folder
        delete_blobs(blob_list)
        print(f'deleted blobs from {site_tmp_folder}')
  
    return None

def is_header_file(file_name):
    '''Check if a file is a header file.'''
    required_tags = ["tmp/", "HEADER"]
    forbidden_tags = ["_boxfolder_", "_fileid_"]
    
    all_required_tags_present = all(tag in file_name for tag in required_tags)
    no_forbidden_tags_present = not any(tag in file_name for tag in forbidden_tags)
    
    # Return TRUE if the file should be exported
    return all_required_tags_present and no_forbidden_tags_present

def generate_output_file_path(input_file_path):
    # Define a regular expression pattern to capture the required parts
    pattern = r'gs://deidentified_site_recruitment_data_prod/([^/]+)/tmp/([^_]+)_deidentified_recruitment_data_box_folder_(\d{12})_file_id_(\d{13})_HEADER_\d*\.csv'

    # Use re.search() to find the matches
    match = re.search(pattern, input_file_path)

    if match:
        # Extract the relevant parts
        healthcare_site_folder = match.group(1)
        healthcare_site = match.group(2)
        box_folder_id = match.group(3)
        file_id = match.group(4)

        # Construct the desired output format
        output_string = f"deidentified_site_recruitment_data_prod/{healthcare_site_folder}/" + \
                        f"{healthcare_site}_deidentified_recruitment_data_boxfolder_{box_folder_id}_fileid_{file_id}.csv"

        return output_string
    else:
        return None  # Return None if no match found in the input string

def list_bucket_blobs(bucket_name, folder_name=None):
    """
    List blobs (objects) in a Google Cloud Storage bucket within a specified folder.
    
    This function retrieves a list of blobs (objects) from a specific folder in a 
    Google Cloud Storage bucket.
    The list of blobs is sorted so that blobs with "HEADER" in their names come first,
    followed by the rest sorted in alphanumeric order.
    
    :param bucket_name: Name of the Cloud Storage bucket.
    :param folder_name: Name of the folder within the bucket (optional).
    :return: A sorted list of blob objects.
    """
    # Initialize a client
    client = storage.Client()

    # Get the bucket
    bucket = client.get_bucket(bucket_name)

    # List all blobs in the bucket
    blob_list = list(bucket.list_blobs(prefix=folder_name))

    # Sort the blob list
    def blob_sort_key(blob):
        blob_name = blob.name
        if "HEADER" in blob_name:
            return (0, blob_name)  # "HEADER" blob comes first
        return (1, blob_name)  # Sort the rest alphabetically
    
    blob_list.sort(key=blob_sort_key)

    return blob_list

def concatenate_blobs(blob_list, target_blob_name):
    """
    Concatenate multiple blobs into a single blob using the Google Cloud Storage 'compose' method.
    
    :param blob_list: A list of blobs from the same folder in a bucket.
    :param target_blob_name: Name of the target blob where the concatenated content will be stored.
    :return: The composed target blob.
    """

    # Get the bucket
    bucket = blob_list[0].bucket

    # Create a list of source blob objects
    source_blobs = [bucket.blob(blob.name) for blob in blob_list]

    # Create a target blob object
    target_blob = bucket.blob(target_blob_name)

    # Compose source blobs into the target blob
    target_blob.compose(source_blobs)

    return target_blob

def delete_blobs(blobs_to_delete):
    """
    Delete a list of blobs (objects) from a Google Cloud Storage bucket.

    :param blobs_to_delete: List of blob objects to be deleted.
    :return: List of deleted blob names.
    """

    deleted_blob_names = []

    for blob in blobs_to_delete:
        try:
            blob.delete()
            deleted_blob_names.append(blob.name)
            print(f"Deleted blob: {blob.name}")
        except Exception as e:
            print(f"Failed to delete blob: {blob.name}, Error: {e}")

    return deleted_blob_names

def list_subdirectories(bucket_name, prefix):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter='/')

    subdirectories = set()
    for blob in blobs:
        subdirectory = blob.name.split('/')[0]
        if subdirectory:
            subdirectories.add(subdirectory)

    return list(subdirectories)


##### Example script for testing #########
# To authenticate in terminal: gcloud auth application-default login

example_file_obj = {'bucket': 'deidentified_site_recruitment_data_prod', 'contentType': 'application/octet-stream', 'crc32c': '0lHnNA==', 'etag': 'CPfbuOaG8YEDEAE=', 'generation': '1697132382662135', 'id': 'deidentified_site_recruitment_data_prod/KaiserPermanente-Hawaii/tmp/KaiserPermanente-Hawaii_deidentified_recruitment_data_box_folder_227960879930_file_id_1318226785372_HEADER_000000000000.csv/1697132382662135', 'kind': 'storage#object', 'md5Hash': '5xpYxbCM3AD4KklAfpLn8w==', 'mediaLink': 'https://storage.googleapis.com/download/storage/v1/b/deidentified_site_recruitment_data_prod/o/KaiserPermanente-Hawaii%2Ftmp%2FKaiserPermanente-Hawaii_deidentified_recruitment_data_box_folder_227960879930_file_id_1318226785372_HEADER_000000000000.csv?generation=1697132382662135&alt=media', 'metageneration': '1', 'name': 'KaiserPermanente-Hawaii/tmp/KaiserPermanente-Hawaii_deidentified_recruitment_data_box_folder_227960879930_file_id_1318226785372_HEADER_000000000000.csv', 'selfLink': 'https://www.googleapis.com/storage/v1/b/deidentified_site_recruitment_data_prod/o/KaiserPermanente-Hawaii%2Ftmp%2FKaiserPermanente-Hawaii_deidentified_recruitment_data_box_folder_227960879930_file_id_1318226785372_HEADER_000000000000.csv', 'size': '7827', 'storageClass': 'STANDARD', 'timeCreated': '2023-10-12T17:39:42.703Z', 'timeStorageClassUpdated': '2023-10-12T17:39:42.703Z', 'updated': '2023-10-12T17:39:42.703Z'}
main(example_file_obj)