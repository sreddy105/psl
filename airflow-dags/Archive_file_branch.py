import re

def execute_query(**kwargs):
    pass

def fetch_feed_file_from_archive(common_prefix: str):
    # find directories with date eg : feeds/standard-feed/20220202/*
    blobs = storage_client.list_blobs(ARCHIVAL_BUCKET, prefix=common_prefix, delimiter="/")
    file_path = [str(blob.name) for blob in blobs]

    # move files back to ingress bucket
    move_file(ARCHIVAL_BUCKET, BUCKET_NAME, source_files=file_path, destination_files=file_path)

    return True



def fetch_manifest_files_from_archive(**kwargs):
    prefix = "manifest"
    blobs = storage_client.list_blobs(ARCHIVAL_BUCKET, prefix=prefix, delimiter="/")
    kv_feed = []
    standard_feed = []

    kv_feed_regex = prefix + "auction_kv_labels_feed-11303-" + transaction_dt.strftime("%Y%m%d") + "\w+"
    standard_feed_regex = prefix + "standard_feed_feed-11303-" + transaction_dt.strftime("%Y%m%d") + "\w+"

    for blob in blobs:
        if re.match(kv_feed_regex, blob.name):
            kv_feed.append(blob.name)
        elif re.match(standard_feed_regex, blob.name):
            standard_feed.append(blob.name)



    # move kv feeds from archival to ingress bucket
    move_file(ARCHIVAL_BUCKET, BUCKET_NAME, source_files=kv_feed, destination_files=kv_feed)

    # move standard feeds from archival to ingress bucket
    move_file(ARCHIVAL_BUCKET, BUCKET_NAME, source_files=standard_feed, destination_files=standard_feed)
    return True

def fetch_kv_feeds_from_archival(**kwargs):

    kv_feed_common_path = "feeds/kv_feed/{date}/".format(date=transaction_dt.strftime("%Y%m%d"))
    fetch_feed_file_from_archive(kv_feed_common_path)


def fetch_standard_feeds_from_archival(**kwargs):
    kv_feed_common_path = "feeds/standard_feed/{date}/".format(date=transaction_dt.strftime("%Y%m%d"))
    fetch_feed_file_from_archive(kv_feed_common_path)



def detect_history_transaction_dt():
    client = None
    select_query = "SELECT TRANSACTION_dt"

    results = execute_query(client, select_query)

    for result in results:
        if result.processed_flag == "N":
            return "skip_restore_archive_tasks"
        else:
            return ['fetch_manifest_files_from_archive', 'fetch_kv_feeds_from_archival', 'fetch_standard_feeds_from_archival']


