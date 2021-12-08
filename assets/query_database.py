#!/usr/bin/env python3
import argparse
import functools
import sys


import boto3
import boto3.dynamodb


RECORD_STATE_CHOICE = [
    'no_task_run',
    'any_task_run',
    'tasks_incompleted',
    'tasks_completed',
    'tasks_completed_not_fully_validated',
    'fully_validated',
]

FILE_STATE_CHOICE = [
    'has_record',
    'no_record',
]

# NOTE: repeated from util/__init__.py
FEXT_FASTQ = {'.fq', '.fq.gz', '.fastq', '.fastq.gz'}
FEXT_BAM = {'.bam'}
FEXT_VCF = {'.vcf.gz', '.gvcf.gz'}
FEXT_ACCEPTED = {*FEXT_FASTQ, *FEXT_BAM, *FEXT_VCF}


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_name', type=str, default='agha-gdr',
            help='DynamoDB database name to query')
    parser.add_argument('--record_state', choices=RECORD_STATE_CHOICE,
            help=f'Query for specific record state')
    parser.add_argument('--submission_name', type=str,
            help='(record_state) Limit query to given submission name')
    parser.add_argument('--active_only', action='store_true',
            help='(record_state) Limit query to active records only')
    parser.add_argument('--file_state', choices=FILE_STATE_CHOICE,
            help=f'File state e.g. has record')
    parser.add_argument('--bucket_name', type=str, default='agha-gdr-staging',
            help='(file_state) S3 bucket name to search for files')
    parser.add_argument('--s3_prefixes', nargs='+', default=[''], type=str,
            help='(file_state) List of S3 prefixes to limit file search')
    args = parser.parse_args()
    if args.record_state and args.file_state:
        parser.error(f'both --record_state or --file_state cannot be provided at once')
    elif not (args.record_state or args.file_state):
        parser.error(f'either --record_state or --file_state must be provided')
    return args


def main():
    # Get command line arguments
    args = get_arguments()

    # Run query for file state or record state
    dynamodb_resource = boto3.resource('dynamodb').Table(args.database_name)
    if args.file_state:
        s3_client = boto3.client('s3')
        result = query_file_state(
            s3_client,
            dynamodb_resource,
            args.file_state,
            args.bucket_name,
            args.s3_prefixes
        )
    elif args.record_state:
        result = query_record_state(
            dynamodb_resource,
            args.record_state,
            args.submission_name,
            args.active_only,
        )
    else:
        assert False

    # Output data
    print(*result['columns'], sep='\t')
    for row in result['data']:
        print(*row, sep='\t')


def query_file_state(s3_client, dynamodb_resource, file_state, bucket_name, s3_prefixes):
    # Discover all input files under prefixes
    input_files = set()
    for s3_prefix in s3_prefixes:
        if not (s3_keys := get_input_files_in_s3_prefix(s3_client, bucket_name, s3_prefix)):
            print(f'warning: no input files found in {s3_prefix}', file=sys.stderr)
            continue
        input_files.update(s3_keys)
    # Retrieve database entries and determine whether the entry set contains the input files
    # NOTE: with expected size of database and number of files to check, it is probably more
    # efficient to download the database and process locally than send queries to DynamoDB.
    data = list()
    database_entries = get_database_entries(dynamodb_resource)
    for s3_key in input_files:
        record_exists = s3_key in database_entries
        if file_state == 'has_record' and record_exists:
            data.append((s3_key, ))
        elif file_state == 'no_record' and not record_exists:
            data.append((s3_key, ))
    return {'columns': ['input_file_s3_key', 'has_record'], 'data': data}


def query_record_state(dynamodb_resource, record_state, submission_name=None, active_only=None):
    # Conditionally construct query expression
    exprs = list()
    if record_state == 'no_task_run':
        exprs.append(boto3.dynamodb.conditions.Attr('ts_validation_job').eq('na'))
    elif record_state == 'any_task_run':
        exprs.append(boto3.dynamodb.conditions.Attr('ts_validation_job').ne('na'))
    elif record_state == 'tasks_incompleted':
        exprs.append(boto3.dynamodb.conditions.Attr('ts_validation_job').ne('na'))
        exprs.append(boto3.dynamodb.conditions.Attr('tasks_completed').eq('no'))
    elif record_state == 'tasks_completed':
        exprs.append(boto3.dynamodb.conditions.Attr('tasks_completed').eq('yes'))
    elif record_state == 'fully_validated':
        exprs.append(boto3.dynamodb.conditions.Attr('tasks_completed').eq('yes'))
        exprs.append(boto3.dynamodb.conditions.Attr('valid_checksum').eq('yes'))
        exprs.append(boto3.dynamodb.conditions.Attr('valid_filetype').eq('yes'))
        exprs.append(boto3.dynamodb.conditions.Attr('index_result').is_in(['succeeded', 'not run']))
    elif record_state == 'tasks_completed_not_fully_validated':
        exprs_any = list()
        exprs_any.append(boto3.dynamodb.conditions.Attr('valid_checksum').eq('no'))
        exprs_any.append(boto3.dynamodb.conditions.Attr('valid_filetype').eq('no'))
        exprs_any.append(boto3.dynamodb.conditions.Attr('index_result').eq('failed'))
        exprs.append(functools.reduce(lambda acc, new: acc | new, exprs_any))
        exprs.append(boto3.dynamodb.conditions.Attr('tasks_completed').eq('yes'))
    else:
        assert False
    if submission_name:
        exprs.append(boto3.dynamodb.conditions.Key('submission_name').begins_with(submission_name))
    if active_only is True:
        exprs.append(boto3.dynamodb.conditions.Attr('active').eq(True))
    elif active_only is not False:
        assert False
    expr_full = functools.reduce(lambda acc, new: acc & new, exprs)
    # Retrieve records
    records = get_records(dynamodb_resource, expr_full)
    # Prepare output data and return
    columns = [
        'partition_key',
        'sort_key',
        'active',
        'tasks_completed',
        'valid_checksum',
        'valid_filetype',
        'index_result',
    ]
    data = list()
    for record in records:
        record_data = [record[field] for field in columns]
        data.append(record_data)
    return {'columns': columns, 'data': data}


def get_input_files_in_s3_prefix(client_s3, bucket_name, prefix):
    metadata = list()
    response = client_s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
    )
    if not (object_mdata := response.get('Contents')):
        return False
    else:
        metadata.extend(object_mdata)
    while response['IsTruncated']:
        token = response['NextContinuationToken']
        response = client_s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            ContinuationToken=token
        )
        metadata.extend(response.get('Contents'))
    input_files = list()
    for mdata in metadata:
        s3_key = mdata['Key']
        if not any(s3_key.endswith(fext) for fext in FEXT_ACCEPTED):
            continue
        input_files.append(s3_key)
    return input_files


def get_database_entries(dynamodb_resource):
    # Get records
    records = list()
    response = dynamodb_resource.scan()
    if 'Items' not in response:
        print(f'error: could not any records in provided table', file=sys.stderr)
        sys.exit(1)
    else:
        records.extend(response.get('Items'))
    while last_result_key := response.get('LastEvaluatedKey'):
        response = DYNAMODB_RESOURCE.scan(
            ExclusiveStartKey=last_result_key,
        )
        records.extend(response.get('Items'))
    # Creating mapping: partition_key -> partition_key (S3 key)
    return {r['partition_key'] for r in records}


def get_records(dynamodb_resource, expr_full):
    records = list()
    response = dynamodb_resource.scan(
        FilterExpression=expr_full,
    )
    if 'Items' not in response:
        print(f'error: could not any records in provided table', file=sys.stderr)
        sys.exit(1)
    else:
        records.extend(response.get('Items'))
    while last_result_key := response.get('LastEvaluatedKey'):
        response = dynamodb_resource.scan(
            FilterExpression=expr_full,
            ExclusiveStartKey=last_result_key,
        )
        records.extend(response.get('Items'))
    return records


if __name__ == '__main__':
    main()
