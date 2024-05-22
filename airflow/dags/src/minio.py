import os
import json
import datetime
import logging
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

def determine_since_date(offset_days):
    if offset_days == -1:
        return 'AllTime' # all time
    if offset_days == 0:
        logger.info("Last pull occured less than 1 day ago. No pull needed")
        return 0
    elif offset_days == 1:
        return 'T'  # last day
    elif offset_days <= 2:
        return 'Y'  # last 2 days
    elif offset_days <= 7:
        return 'W'  # last week
    elif offset_days <= 30:
        return 'M'  # last month
    else:
        return None  # No matching sinceDate criteria


class MinioConnection():
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    METADATA_BUCKET_NAME = "metadata"
    PULLDATA_BUCKET_NAME = "pulldata"
    LAST_PULL_OBJECT = "last_pull"
    PROCESSED_FILES_HISTORY = "processed_files"
    def __init__(self) -> None:
        host = "myminio"
        port = "9000"
        access_key=os.environ.get('MINIO_ACCESS_KEY')
        secret_key=os.environ.get('MINIO_SECRET_KEY')
        
        try:
            self._client = Minio(
                host+':'+port,
                access_key,
                secret_key,
                secure=False
            )
            logger.info("Minio successfully connected.")
        except:
            logger.error("Failed to connect to minio.")
    
    def update_last_pull(self):
        try:
            current_date = datetime.datetime.now()
            date_str = current_date.strftime(self.DATETIME_FORMAT)
            self.write_object(self.METADATA_BUCKET_NAME, self.LAST_PULL_OBJECT, date_str)
            logger.info("Last pull date updated successfully.")
        except:
            logger.warn("Couldn't update last pull date.")
        
    
    def get_timedelta_from_last_pull(self):
        # Create bucket if it doesnt exist
        bucket_name = self.METADATA_BUCKET_NAME
        last_pull_objectname = self.LAST_PULL_OBJECT
        
        if not self._client.bucket_exists(bucket_name):
            self._client.make_bucket(bucket_name)
            
        read_date_str = self.read_object(bucket_name, last_pull_objectname)
        if read_date_str is not None:
            
            # Parse and return offset in days
            read_date = datetime.datetime.strptime(read_date_str, self.DATETIME_FORMAT)
            offset_delta = datetime.datetime.now() - read_date
            offset_days = offset_delta.days
            
        else:
            # Determine current date. Save it to file and put in minio storage.
            self.update_last_pull()
            offset_days = -1
            
        delta = determine_since_date(offset_days)
        return delta
        
        
    def import_processed_history(self):
        self._processed_data = []
        metadata_bucket = self.METADATA_BUCKET_NAME
        processed_files_object= self.PROCESSED_FILES_HISTORY
        
        # Read and process filenames that have been already processed.
        processed_data = self.read_object(metadata_bucket, processed_files_object)
        
        self._processed_data = processed_data if processed_data is not None else []
        
        
    def read_unprocessed_data(self):
        
        # Read which files were already processed.
        logger.info(f"Start reading unprocessed data")
        self.import_processed_history()
        
        data_bucket_name = self.PULLDATA_BUCKET_NAME
        if not self._client.bucket_exists(data_bucket_name):
            logger.error(f"Bucket {data_bucket_name} does not exist, Cannot read data.")
            
        unprocessed_objects = []
        unprocessed_data = []
        objects = self._client.list_objects(data_bucket_name)
        for object in objects:
            if object.object_name in self._processed_data:
                # This object was already processed. Skip it.
                logger.info(f"Object {object.object_name} was already processed, skipping ...")
                continue
            
            data = json.loads(self.read_object(data_bucket_name, object.object_name))
            unprocessed_data.append(data)
            unprocessed_objects.append(object.object_name)
            
        return unprocessed_data, unprocessed_objects
        
    
    def read_object(self, bucket_name, object_name):
        file_name = os.environ.get('AIRFLOW_TMP_DATA_PATH') + '/' \
            + object_name
        logger.info(f"Start reading data from object name {object_name}")
        data = None
        try:
            # Retrieve from minio. Read.
            self._client.fget_object(
                bucket_name, object_name, file_name
            )
            with open(file_name, 'r') as file:
                data = file.read()
                
        except (FileNotFoundError, S3Error) as e:
            if e is S3Error:
                if e.code != 'NoSuchKey':
                    logger.error(f"Error in getting last pull date occured: {e}")
                else:
                    logger.info(f"File {object_name} couldn't be read.")
        return data
    
    
    def write_object(self, bucket_name, object_name, data, append=False):
        file_name = os.environ.get('AIRFLOW_TMP_DATA_PATH') + '/' \
            + object_name
        logger.info(f"Start writing data to file name {object_name}")
        
        try:
            if append:
                try:
                    self._client.fget_object(
                        bucket_name, object_name, file_name
                    )
                except (FileNotFoundError, S3Error) as e:
                    # Catch here really bad error. Otherwise - file not found which is ok.
                    if e is S3Error:
                        if e.code != 'NoSuchKey':
                            logger.error(f"Error in getting file for append write occured: {e}")
                    
                    # Create a file since none exists.
                    with open(file_name, 'w') as file:
                        pass
                
                # Append data
                with open(file_name, 'a') as file:
                    file.write(str(data)+'\n')
                
            else:
                with open(file_name, 'w') as file:
                    if isinstance(data, dict):
                        json.dump(data, file)
                    else:
                        file.write(data)
                    
            self._client.fput_object(
                bucket_name, object_name, file_name
            )
            logger.info(f"Successfully save filename to object: {object_name}.")
            
        except Exception as e:
            logger.error(f"Couldn't save filename to object: {object_name}.\nError {e}")
        
    
    