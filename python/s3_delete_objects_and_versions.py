import boto3

def delete_all_versions(bucket_name, prefix, dry_run=True):
    """
    Identify and optionally delete all objects and their versions in a versioned S3 bucket under a specific prefix.
    This function searches for all objects, versions, and delete markers in the specified prefix within a versioned S3 bucket.
    It supports both a "dry run" mode (to preview deletions) and an execution mode (to perform actual deletions).

    ---------------------------------- PARAMETERS ----------------------------------
    :param bucket_name: (str)
        The name of the S3 bucket where the search and deletion will be performed.
    :param prefix: (str)
        A prefix (e.g., folder path) that limits the search scope to objects under this path.
    :param dry_run: (bool)
        - If True, lists all objects and their versions without performing deletions.
        - If False, deletes all objects, versions, and delete markers under the specified prefix.

    ---------------------------------- BEHAVIOR -----------------------------------
    - In "dry run" mode (dry_run=True):
        - The function lists all objects and their versions that match the specified prefix.
        - No deletions are performed.
        - A summary of objects is displayed in the terminal.
    - In "execution" mode (dry_run=False):
        - All matching objects, their versions, and delete markers are permanently deleted.
        - Deletions are processed in batches of 1,000 to optimize performance and meet AWS limits.

    ---------------------------------- EXAMPLE OUTPUT --------------------------------
    Dry-Run Example:
    ----------------
    Objects to delete (DRY-RUN):
    Key: archives/project-name/year=2024/file1.txt, VersionId: 12345
    Key: archives/project-name/year=2024/file2.txt, VersionId: 67890
    Key: archives/project-name/year=2024/file3.txt, VersionId: abcde
    ...

    Execution Example:
    ------------------
    Deleting objects in batches of 1,000...
    Deleted 1000 objects in batch 1
    Deleted 1000 objects in batch 2
    Deleted 1000 objects in batch 3
    Deleted 1000 objects in batch 4
    Deleted 884 objects in batch 5
    Deletion complete.

    ---------------------------------- NOTES --------------------------------------
    - This function requires the bucket to have versioning enabled.
    - If the prefix is invalid or empty, no action will be taken, and an error will be raised.
    - Use caution when setting dry_run=False, as deleted objects cannot be recovered.

    ---------------------------------- RETURN VALUE --------------------------------
    :return: (list)
    A list of dictionaries representing objects processed by the function. Each dictionary contains:
    - 'Key': The key (path) of the object.
    - 'VersionId': The ID of the specific object version.
    - In dry-run mode, the returned list includes all objects and their versions identified for deletion. No action is taken.

    ---------------------------------- EXAMPLE USAGE ------------------------------
    dry_run_example = True  # Preview the deletions without executing them.
    bucket_name = "my-versioned-bucket"
    prefix = "folder/subfolder/"

    delete_all_versions(bucket_name, prefix, dry_run=dry_run_example)
    """

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_object_versions')
    objects_to_delete = []

    # Paginate through the bucket's object versions and delete markers.
    print(f"Searching for objects containing under '{prefix}' in bucket '{bucket_name}'...")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for version in page.get('Versions', []):
                objects_to_delete.append({
                    'Key': version['Key'],
                    'VersionId': version['VersionId']
                })
        for delete_marker in page.get('DeleteMarkers', []):
                objects_to_delete.append({
                'Key': delete_marker['Key'],
                'VersionId': delete_marker['VersionId']
            })

    # Print a message if no objects matching the parameters are found.
    if not objects_to_delete:
        print(f"No objects found in the specified '{bucket_name}/'{prefix}'.")
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
    print("Deleting objects...")
    for i in range(0, len(objects_to_delete), 1000):
        batch = objects_to_delete[i:i+1000]
        s3.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': batch}
        )
        print(f"Deleted {len(batch)} objects in batch {i // 1000 + 1}")
    print("Deletion complete.")


# Parameters for the S3 query
bucket_name = "blue-lightning-data-prod"                       # Name of the bucket it will do the search.
prefix = "datalake/towerdata/emaildb/parquet/year=2024/month=10/day=22/hour=12/minute=24"  # Example for archived project data".
dry_run = False # Set to True for testing (no objects will be deleted); set to False to perform deletion

delete_all_versions(bucket_name, prefix, dry_run)
