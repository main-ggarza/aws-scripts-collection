import boto3

def list_and_delete_objects(bucket_name, prefix, search_word, dry_run=True):

    """
    Search and optionally delete objects in a versioned S3 bucket based on a prefix and search word.
    This function identifies objects and versions in a versioned S3 bucket that meet the following criteria:
    - They reside under the specified prefix (e.g., folder path).
    - Their keys contain the specified search word.

    For each matching object, including all versions and delete markers, the function can:
    - List the objects and their versions (if dry_run=True).
    - Permanently delete the objects and their versions (if dry_run=False).

    ---------------------------------- PARAMETERS ----------------------------------
    :param bucket_name: (str) The name of the S3 bucket to search.
    :param prefix: (str) A prefix (e.g., folder path) to limit the search scope.
    :param search_word: (str) A substring to match within object keys.
    :param dry_run: (bool)
        - If True, lists objects to delete without performing any deletions.
        - If False, deletes matching objects and their versions.

    ---------------------------------- NOTES ---------------------------------------
    - This function only works with versioned S3 buckets.
    - If no matching objects are found, a message is displayed, and no action is taken.
    - Use caution when setting dry_run= False, as deletions cannot be undone.
    """

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_object_versions')

    objects_to_delete = []

    # Paginate through the bucket's object versions and delete markers.
    print(f"Searching for objects containing '{search_word}' under '{prefix}' in bucket '{bucket_name}'...")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for version in page.get('Versions', []):
            if search_word in version['Key']:
                objects_to_delete.append({
                    'Key': version['Key'],
                    'VersionId': version['VersionId']
                })
        for delete_marker in page.get('DeleteMarkers', []):
            if search_word in delete_marker['Key']:
                objects_to_delete.append({
                'Key': delete_marker['Key'],
                'VersionId': delete_marker['VersionId']
            })

    # Print a message if no objects matching the parameters are found.
    if not objects_to_delete:
        print(f"No objects found containing '{search_word}' in the specified '{bucket_name}/'{prefix}'.")
        return

    # Print all objects and their versions identified for deletion.
    print(f"Objects to delete ({'DRY-RUN' if dry_run else 'EXECUTION'}):")
    for obj in objects_to_delete:
        print(f"  Key: {obj['Key']}, VersionId: {obj['VersionId']}")

    # Skip deletion if dry_run is enabled.
    if dry_run:
        print("Dry-run mode enabled. No objects have been deleted.")
        return

    # Deleting all S3 object versions and delete markers matching the specified parameters
    print("Deleting S3 Objects...")
    for obj in objects_to_delete:
        print(f"Deleting Key: {obj['Key']}, VersionId: {obj['VersionId']}")
        s3.delete_object(Bucket=bucket_name, Key=obj['Key'], VersionId=obj['VersionId'])
    print("Deletion complete.")

# Parameters for the S3 query
bucket_name = "my_bucket_name"                       # Name of the bucket it will do the search.
prefix = "archives/project-name-example/year=2024/"  # Example for archived project data".
search_word = "error"                                # Search for object keys containing the word 'error'.
dry_run = True # Set to True for testing (no objects will be deleted); set to False to perform deletion

list_and_delete_objects(bucket_name, prefix, search_word, dry_run)
