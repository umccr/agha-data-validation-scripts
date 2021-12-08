#!/usr/bin/env python3
import argparse
import decimal
import enum
import json
import logging
import os
import pathlib
import sys


import boto3
import util
from util import s3, dynamodb


# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
# Add log file handler so we can upload log messages to S3
LOG_FILE_NAME = 'log.txt'
LOG_FILE_HANDLER = util.FileHandlerNewLine(LOG_FILE_NAME)
LOGGER.addHandler(LOG_FILE_HANDLER)

# Get environment variables
DYNAMODB_STAGING_TABLE_NAME = util.get_environment_variable('DYNAMODB_STAGING_TABLE_NAME')
RESULTS_BUCKET = util.get_environment_variable('RESULTS_BUCKET')
STAGING_BUCKET = util.get_environment_variable('STAGING_BUCKET')
RESULTS_KEY_PREFIX = util.get_environment_variable('RESULTS_KEY_PREFIX')

# Emitted to log for record purposes
BATCH_JOBID = util.get_environment_variable('AWS_BATCH_JOB_ID')

# Get AWS clients
RESOURCE_DYNAMODB = util.get_dynamodb_table_resource(DYNAMODB_STAGING_TABLE_NAME, region_name='ap-southeast-2')
CLIENT_S3 = util.get_client('s3')

class BatchJobResult:

    def __init__(self, staging_s3_key='', type='', value='', status='', filename=[]):
        self.staging_s3_key=staging_s3_key
        self.type = type
        self.value = value
        self.status = status
        self.filename = filename

class Tasks(enum.Enum):

    CHECKSUM = 'checksum'
    FILE_VALIDATE = 'validate_filetype'
    INDEX = 'create_index'


class FileTypes(enum.Enum):

    BAM = 'BAM'
    FASTQ = 'FASTQ'
    VCF = 'VCF'

    @classmethod
    def contains(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


INDEXABLE_FILES =  {
    FileTypes.BAM,
    FileTypes.VCF,
}

def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--partition_key', required=True, type=str,
            help='DynamoDB partition key used to identify file (s3_key expected)')
    parser.add_argument('--tasks', required=True, choices=[m.value for m in Tasks], nargs='+',
            help='Tasks to perform')
    return parser.parse_args()


def main():
    # Get command line arguments
    args = get_arguments()

    # Log Batch job id and set job datetime stamp
    LOGGER.info(f'starting job: {BATCH_JOBID}')

    # Parsing values
    staging_s3_key = args.partition_key
    filename = s3.get_s3_filename_from_s3_key(staging_s3_key)
    LOGGER.info(f'Grabbing filename: {filename}')

    # Grab dynamodb staging record
    file_record = get_record(staging_s3_key)
    LOGGER.info('File record')
    LOGGER.info(json.dumps(file_record))

    # Grab list result
    batch_job_result_list = []

    # Stage file from S3 and then validate
    fp_local = stage_file(
        staging_bucket=STAGING_BUCKET,
        s3_key=staging_s3_key,
        filename=filename
    )

    tasks = {Tasks(task_str) for task_str in args.tasks}

    # Checksum tasks
    if Tasks.CHECKSUM in tasks:
        checksum_result = run_checksum(fp_local, file_record)
        batch_job_result_list.append(checksum_result.__dict__)
        LOGGER.info('Appending results:')
        LOGGER.info(json.dumps(batch_job_result_list))

    # File Validation task
    if Tasks.FILE_VALIDATE in tasks:
        file_validation_result = run_filetype_validation(fp_local, file_record)
        batch_job_result_list.append(file_validation_result.__dict__)
        LOGGER.info('Appending results:')
        LOGGER.info(json.dumps(batch_job_result_list))

        filetype=file_validation_result.value

        # Simplify index requirement check
        if Tasks.INDEX in tasks and filetype in INDEXABLE_FILES:
            indexing_result = run_indexing(fp_local, file_record, filetype)
            batch_job_result_list.append(indexing_result.__dict__)
            LOGGER.info('Appending results:')
            LOGGER.info(json.dumps(batch_job_result_list))

    # Write completed result to log and S3
    write_results_s3(batch_job_result_list, staging_s3_key)


def get_record(partition_key):

    response = RESOURCE_DYNAMODB.get_item(
        Key={'partition_key': partition_key, 'sort_key': dynamodb.FileRecordSortKey.MANIFEST_FILE_RECORD.value}
    )
    if 'Item' not in response:
        msg_key_text = f'partition key {partition_key}.'
        LOGGER.critical(f'could not retrieve DynamoDB entry with {msg_key_text}')
        sys.exit(1)
    record_raw = response.get('Item')
    return replace_record_decimal_object(record_raw)


def replace_record_decimal_object(record):
    for k in record:
        if isinstance(record[k], decimal.Decimal):
            record[k] = int(record[k]) if record[k] % 1 == 0 else float(record[k])
    return record


def stage_file(staging_bucket, s3_key, filename):
    LOGGER.info(f'staging file from S3: s3://{staging_bucket}/{s3_key}')
    output_fp = pathlib.Path(filename)
    with output_fp.open('wb') as fh:
        CLIENT_S3.download_fileobj(staging_bucket, s3_key, fh)
    return output_fp


def run_checksum(fp, file_record) -> BatchJobResult:
    """
    Expected result: A record class of BatchJobResult
    """

    LOGGER.info('running checksum')

    # Some environment
    check_type = "checksum"
    staging_s3_key = file_record["partition_key"]
    provided_checksum = file_record["provided_checksum"]

    # Execute checksum
    command = f"md5sum {fp} | cut -f1 -d' '"
    LOGGER.info(f'Command to execute: {command}')
    result = util.execute_command(command)

    caluclated_checksum = result.stdout.rstrip()

    # Create result class
    batch_job_result = BatchJobResult(staging_s3_key=staging_s3_key, type=check_type, value=caluclated_checksum)

    if result.returncode != 0:
        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr: {result.stderr}'
        LOGGER.critical(f'failed to run checksum ({command}): {stdstrm_msg}')

        batch_job_result.status='FAIL'

        return batch_job_result
    
    # Update status result
    if caluclated_checksum == provided_checksum:
        batch_job_result.status = "PASS"
    else:
        batch_job_result.status = "FAIL"
    
    # Log results
    provided_str = f'provided:   {provided_checksum}'
    calculated_str  = f'calculated: {caluclated_checksum}'
    validated_str = f'validated:  {batch_job_result.status}'
    checksum_str = f'{provided_str}\r\t{calculated_str}\r\t{validated_str}'
    LOGGER.info(f'checksum results:\r\t{checksum_str}')

    # Return value
    return batch_job_result


def run_filetype_validation(fp, file_record) -> BatchJobResult:
    LOGGER.info('running file type validation')

    # Some environment
    check_type = "filetype"
    staging_s3_key = file_record["partition_key"]

    # Create result class
    batch_job_result = BatchJobResult(staging_s3_key=staging_s3_key, type=check_type)

    # Get file type
    if any(fp.name.endswith(fext) for fext in util.FEXT_BAM):
        filetype = FileTypes.BAM
        command = f'samtools quickcheck -q {fp}'
    elif any(fp.name.endswith(fext) for fext in util.FEXT_FASTQ):
        filetype = FileTypes.FASTQ
        command = f'fqtools validate {fp}'
    elif any(fp.name.endswith(fext) for fext in util.FEXT_VCF):
        filetype = FileTypes.VCF
        command = f'bcftools query -l {fp}'
    else:
        LOGGER.critical(f'could not infer file type from extension for {fp}')
        batch_job_result.status='FAIL'
        write_results_s3(batch_job_result.__dict__, staging_s3_key)
        sys.exit(1)

    # Validate filetype
    batch_job_result.value = filetype.value
    LOGGER.info(f'Command to execute: {command}')
    result = util.execute_command(command)
    if result.returncode != 0:

        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr: {result.stderr}'
        LOGGER.info(f'file validation failed (invalid filetype or other failure): {stdstrm_msg}')

        # Update status
        batch_job_result.status = 'FAIL'

        # Write log and exit
        return batch_job_result

    else:
        # Update status
        batch_job_result.status = 'PASS'

    # Log results
    inferred_str  = f'inferred:  {batch_job_result.value}'
    validated_str = f'validated:  {batch_job_result.status}'
    filetype_str = f'{inferred_str}\r\t{validated_str}'
    LOGGER.info(f'file type validation results:\r\t{filetype_str}')

    return batch_job_result


def run_indexing(fp, file_record, filetype) -> BatchJobResult:

    # Some environment
    check_type = "index"
    staging_s3_key = file_record["partition_key"]

    # Create result class
    batch_job_result = BatchJobResult(staging_s3_key=staging_s3_key, type=check_type)

    # Run appropriate indexing command
    LOGGER.info('running indexing')
    if filetype == FileTypes.BAM:
        command = f'samtools index {fp}'
        index_fp = f'{fp}.bai'
    elif filetype == FileTypes.VCF:
        command = f"tabix {fp} -p 'vcf'"
        index_fp = f'{fp}.tbi'
    else:
        # You should never have come here
        assert False

    result = util.execute_command(command)

    if result.returncode != 0:
        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr: {result.stderr}'
        LOGGER.critical(f'failed to run indexing ({command}): {stdstrm_msg}')

        batch_job_result.status = 'FAIL'
        return batch_job_result

    # Upload index and set results
    index_s3_key = upload_index(index_fp)

    batch_job_result.status = 'PASS'
    batch_job_result.value = index_s3_key

    # Log results
    result_str = f'result:    {batch_job_result.status}'
    filename_str = f'filename:  {os.path.basename(index_s3_key)}'
    bucket_str = f'S3 bucket: {RESULTS_BUCKET}'
    key_str = f'S3 key:    {index_s3_key}'
    filetype_str = f'{result_str}\r\t{filename_str}\r\t{bucket_str}\r\t{key_str}'
    LOGGER.info(f'file indexing results:\r\t{filetype_str}')

    return batch_job_result


def get_results_data_s3_key(s3_key):
    filename = s3.get_s3_filename_from_s3_key(s3_key)
    s3_key_fn = f'{filename}__results.json'
    return os.path.join(RESULTS_KEY_PREFIX, s3_key_fn)


def get_log_s3_key(s3_key):
    filename = s3.get_s3_filename_from_s3_key(s3_key)
    s3_key_fn = f'{filename}__log.txt'
    return os.path.join(RESULTS_KEY_PREFIX, s3_key_fn)


def upload_index(index_fp):
    s3_key = os.path.join(RESULTS_KEY_PREFIX, index_fp)
    LOGGER.info(f'writing index to s3://{RESULTS_BUCKET}/{s3_key}')
    CLIENT_S3.upload_file(index_fp, RESULTS_BUCKET, s3_key)
    return s3_key


def write_results_s3(batch_result, staging_s3_key):
    # Create results json
    s3_object_body = f'{json.dumps(batch_result, indent=4)}\n'

    # Upload results and log to S3
    s3_key = get_results_data_s3_key(staging_s3_key)
    s3_object_body_log = s3_object_body.replace('\n', '\r')

    LOGGER.info(f'writing results to s3://{RESULTS_BUCKET}/{s3_key}:\r{s3_object_body_log}')
    CLIENT_S3.put_object(Body=s3_object_body, Bucket=RESULTS_BUCKET, Key=s3_key)
    upload_log(staging_s3_key)

def upload_log(staging_s3_key):
    s3_key = get_log_s3_key(staging_s3_key)
    LOGGER.info(f'writing log file to s3://{RESULTS_BUCKET}/{s3_key}')
    LOG_FILE_HANDLER.flush()
    CLIENT_S3.upload_file(LOG_FILE_NAME, RESULTS_BUCKET, s3_key)


if __name__ == '__main__':
    main()
