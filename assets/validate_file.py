#!/usr/bin/env python3
import argparse
import decimal
import json
import logging
import os
import pathlib
import gzip
import shutil
import sys

import util
import util.batch as batch
import util.s3 as s3
import util.agha as agha

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
# Add log file handler so we can upload log messages to S3
LOG_FILE_NAME = 'log.txt'
LOG_FILE_HANDLER = util.FileHandlerNewLine(LOG_FILE_NAME)
LOGGER.addHandler(LOG_FILE_HANDLER)

# Get environment variables
RESULTS_BUCKET = util.get_environment_variable('RESULTS_BUCKET')
STAGING_BUCKET = util.get_environment_variable('STAGING_BUCKET')
RESULTS_KEY_PREFIX = util.get_environment_variable('RESULTS_KEY_PREFIX')

# Emitted to log for record purposes
BATCH_JOBID = util.get_environment_variable('AWS_BATCH_JOB_ID')

# Get AWS clients
CLIENT_S3 = util.get_client('s3')


# Aim:
# .vcf.gz
# .vcf.gz.tbi


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--s3_key', required=True, type=str,
                        help='Staging s3_key is expected')
    parser.add_argument('--checksum', required=True, type=str,
                        help='Checksum for the s3_key staging file')
    parser.add_argument('--tasks', required=True, choices=[m.value for m in batch.Tasks], nargs='+',
                        help='Tasks to perform')
    return parser.parse_args()


def main():
    # Get command line arguments
    args = get_arguments()

    # Log Batch job id and set job datetime stamp
    LOGGER.info(f'starting job: {BATCH_JOBID}')

    # Parsing values
    provided_checksum = args.checksum
    staging_s3_key = args.s3_key
    tasks = {batch.Tasks(task_str) for task_str in args.tasks}

    LOGGER.info(f'Processing: \n Checksum: {(provided_checksum)}, '
                f'staging_s3_key:{(staging_s3_key)}, tasks:{(tasks)}')

    # Grab list result
    batch_job_result_list = []

    # Stage file from S3 and then validate
    filename = s3.get_s3_filename_from_s3_key(staging_s3_key)
    LOGGER.info(f'Grabbing filename: {filename}')
    fp_local = stage_file(
        staging_bucket=STAGING_BUCKET,
        s3_key=staging_s3_key,
        filename=filename
    )

    # Checksum tasks
    if batch.Tasks.CHECKSUM_VALIDATION in tasks:
        checksum_result = run_checksum(fp_local, provided_checksum, staging_s3_key)
        batch_job_result_list.append(checksum_result.__dict__)
        LOGGER.info('Appending results:')
        LOGGER.info(json.dumps(checksum_result.__dict__))

        # Not proceeding to anything else. Original data might have something wrong
        if checksum_result.status == batch.StatusBatchResult.FAIL.value:
            LOGGER.info('Checksum test FAIL. Aborting...')
            write_results_s3(batch_job_result_list, staging_s3_key)
            return

    # File Validation task
    if batch.Tasks.FILE_VALIDATION in tasks:
        file_validation_result = run_filetype_validation(fp_local, staging_s3_key)
        batch_job_result_list.append(file_validation_result.__dict__)
        LOGGER.info('Appending results:')
        LOGGER.info(json.dumps(file_validation_result.__dict__))

        # Not proceeding to anything else. Original data might have something wrong
        if file_validation_result.status == batch.StatusBatchResult.FAIL.value:
            LOGGER.info('File validation test FAIL. Aborting...')
            write_results_s3(batch_job_result_list, staging_s3_key)
            return

        filetype = file_validation_result.value

        # Compression Task
        if batch.Tasks.COMPRESS in tasks and agha.FileType.is_compressable(filetype):
            LOGGER.info(f'Compressing job has been selected')

            if not staging_s3_key.endswith('.gz'):
                LOGGER.info(f'Data need to be compressed. Running it now.')
                compression_result = run_compression(fp_local=fp_local, staging_s3_key=staging_s3_key)
            else:
                LOGGER.info(f'Data is already compressed. Compressed result refer to staging original data')
                source_file = {
                    'bucket_name': STAGING_BUCKET,
                    's3_key': staging_s3_key,
                    'checksum': provided_checksum
                }

                compression_result = batch.BatchJobResult(staging_s3_key=staging_s3_key,
                                                          task_type=batch.Tasks.COMPRESS.value,
                                                          value='FILE', status='SUCCEED',
                                                          source_file=[source_file])

            batch_job_result_list.append(compression_result.__dict__)
            LOGGER.info('Appending results:')
            LOGGER.info(json.dumps(compression_result.__dict__))


        # Simplify index requirement check
        if batch.Tasks.INDEX in tasks and agha.FileType.is_indexable(filetype):
            LOGGER.info(f'Indexing job has been selected')

            indexing_result = run_indexing(fp_local, staging_s3_key, filetype)
            batch_job_result_list.append(indexing_result.__dict__)

            LOGGER.info('Appending results:')
            LOGGER.info(json.dumps(indexing_result.__dict__))

    # Write completed result to log and S3
    write_results_s3(batch_job_result_list, staging_s3_key)


def run_compression(fp_local, staging_s3_key) -> batch.BatchJobResult:
    LOGGER.info('Running compression job')

    # Create result class
    batch_job_result = batch.BatchJobResult(staging_s3_key=staging_s3_key, task_type=batch.Tasks.COMPRESS.value)

    compress_fp = compressing_file(fp_local)
    LOGGER.info(f'Compress file from {fp_local} to {compress_fp}')

    # Uploading data
    compress_s3_key = upload_file_to_s3(compress_fp)

    batch_job_result.status = batch.StatusBatchResult.SUCCEED.value
    batch_job_result.value = 'FILE'

    source_file = {
        'bucket_name': RESULTS_BUCKET,
        's3_key': compress_s3_key,
        'checksum': calculate_checksum_from_fp(compress_fp)
    }
    batch_job_result.source_file = [source_file]

    # Log results
    result_str = f'result:    {batch_job_result.status}'
    filename_str = f'filename:  {os.path.basename(compress_s3_key)}'
    bucket_str = f'S3 bucket: {RESULTS_BUCKET}'
    key_str = f'S3 key:    {compress_s3_key}'
    filetype_str = f'{result_str}\r\t{filename_str}\r\t{bucket_str}\r\t{key_str}'
    LOGGER.info(f'file indexing results:\r\t{filetype_str}')

    return batch_job_result


def compressing_file(fp_in):
    fp_out = str(fp_in) + '.gz'
    with open(fp_in, 'rb') as f_in:
        with gzip.open(fp_out, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return fp_out


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


def run_checksum(fp, provided_checksum, staging_s3_key) -> batch.BatchJobResult:
    """
    Expected result: A record class of BatchJobResult
    """

    LOGGER.info('running checksum')

    # Create result class
    batch_job_result = batch.BatchJobResult(staging_s3_key=staging_s3_key, task_type=batch.Tasks.CHECKSUM_VALIDATION.value)

    try:
        calculated_checksum: str = calculate_checksum_from_fp(fp)

        batch_job_result.value = calculated_checksum

    except ValueError as e:
        stdstrm_msg = f'\r\tstderr: {e}'
        LOGGER.critical(f'failed to run checksum command: {stdstrm_msg}')

        batch_job_result.status = batch.StatusBatchResult.FAIL.value
        return batch_job_result

    # Check for mismatch checksum
    if provided_checksum != calculated_checksum:

        LOGGER.critical(f'Provided checksum and calculated checksum does not match')
        LOGGER.critical(f'Provided checksum: {provided_checksum} and calculated checksum: {calculated_checksum}')

        batch_job_result.status = batch.StatusBatchResult.FAIL.value
        return batch_job_result

    # Checksum should be ok and match
    calculated_str = f'calculated: {calculated_checksum}'
    LOGGER.info(f'Calculated checksum result:\r\t{calculated_str}')

    batch_job_result.status = batch.StatusBatchResult.SUCCEED.value
    return batch_job_result


def run_filetype_validation(fp, staging_s3_key) -> batch.BatchJobResult:
    LOGGER.info('running file type validation')

    # Create result class
    batch_job_result = batch.BatchJobResult(staging_s3_key=staging_s3_key, task_type=batch.Tasks.FILE_VALIDATION.value)

    # Get file type
    if any(fp.name.endswith(fext) for fext in util.FEXT_BAM):
        filetype = agha.FileType.BAM.get_name()
        command = f'samtools quickcheck -q {fp}'

    elif any(fp.name.endswith(fext) for fext in util.FEXT_FASTQ):
        filetype = agha.FileType.FASTQ.get_name()
        command = f'fqtools validate {fp}'

    elif any(fp.name.endswith(fext) for fext in util.FEXT_VCF):
        filetype = agha.FileType.VCF.get_name()
        command = f'bcftools query -l {fp}'

    else:
        LOGGER.critical(f'could not infer file type from extension for {fp}')
        batch_job_result.status = batch.StatusBatchResult.FAIL.value
        return batch_job_result

    # Validate filetype
    batch_job_result.value = filetype
    LOGGER.info(f'Command to execute: {command}')
    result = util.execute_command(command)
    if result.returncode != 0:

        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr: {result.stderr}'
        LOGGER.info(f'file validation failed (invalid filetype or other failure): {stdstrm_msg}')

        # Update status
        batch_job_result.status = batch.StatusBatchResult.FAIL.value

        # Write log and exit
        return batch_job_result

    else:
        # Update status
        batch_job_result.status = batch.StatusBatchResult.SUCCEED.value

    # Log results
    inferred_str = f'inferred:  {batch_job_result.value}'
    validated_str = f'validated:  {batch_job_result.status}'
    filetype_str = f'{inferred_str}\r\t{validated_str}'
    LOGGER.info(f'file type validation results:\r\t{filetype_str}')

    return batch_job_result


def run_indexing(fp, staging_s3_key, filetype) -> batch.BatchJobResult:
    # Create result class
    batch_job_result = batch.BatchJobResult(staging_s3_key=staging_s3_key, task_type=batch.Tasks.INDEX.value)

    # Run appropriate indexing command
    LOGGER.info('running indexing')
    if filetype == agha.FileType.BAM.get_name():
        command = f'samtools index {fp}'
        index_fp = f'{fp}.bai'
    elif filetype == agha.FileType.VCF.get_name():
        command = f"tabix {fp} -p 'vcf'"
        index_fp = f'{fp}.tbi'
    else:
        # You should never have come here
        assert False

    result = util.execute_command(command)

    if result.returncode != 0:
        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr: {result.stderr}'
        LOGGER.critical(f'failed to run indexing ({command}): {stdstrm_msg}')

        batch_job_result.status = batch.StatusBatchResult.FAIL.value
        print(result)
        sys.exit(1)
        return batch_job_result

    # Upload index and set results
    index_s3_key = upload_file_to_s3(index_fp)

    batch_job_result.status = batch.StatusBatchResult.SUCCEED.value
    batch_job_result.value = 'FILE'

    source_file = {
        'bucket_name': RESULTS_BUCKET,
        's3_key': index_s3_key,
        'checksum': calculate_checksum_from_fp(index_fp)
    }

    batch_job_result.source_file = [source_file]

    # Log results
    result_str = f'result:    {batch_job_result.status}'
    filename_str = f'filename:  {os.path.basename(index_s3_key)}'
    bucket_str = f'S3 bucket: {RESULTS_BUCKET}'
    key_str = f'S3 key:    {index_s3_key}'
    filetype_str = f'{result_str}\r\t{filename_str}\r\t{bucket_str}\r\t{key_str}'
    LOGGER.info(f'file indexing results:\r\t{filetype_str}')

    return batch_job_result


def calculate_checksum_from_fp(fp):
    # Execute checksum
    command = f"md5sum {fp} | cut -f1 -d' '"
    LOGGER.info(f'Command to execute: {command}')
    result = util.execute_command(command)

    calculated_checksum: str = result.stdout.rstrip()

    if result.returncode != 0:
        raise ValueError(result.stderr)

    return calculated_checksum


def get_results_data_s3_key(s3_key):
    filename = s3.get_s3_filename_from_s3_key(s3_key)
    s3_key_fn = f'{filename}__results.json'
    return os.path.join(RESULTS_KEY_PREFIX, s3_key_fn)


def get_log_s3_key(s3_key):
    filename = s3.get_s3_filename_from_s3_key(s3_key)
    s3_key_fn = f'{filename}__log.txt'
    return os.path.join(RESULTS_KEY_PREFIX, s3_key_fn)


def upload_file_to_s3(fp):
    s3_key = os.path.join(RESULTS_KEY_PREFIX, fp)
    LOGGER.info(f'writing {fp} to s3://{RESULTS_BUCKET}/{s3_key}')
    CLIENT_S3.upload_file(fp, RESULTS_BUCKET, s3_key)
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
