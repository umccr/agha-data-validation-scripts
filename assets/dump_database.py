#!/usr/bin/env python3
import argparse
import sys


import boto3
import boto3.dynamodb


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_name', required=True, type=str,
            help='DynamoDB database name to query')
    return parser.parse_args()


def main():
    # Get command line arguments
    args = get_arguments()

    # Create Boto3 DynamoDB resource and make query
    dynamodb_resource = boto3.resource('dynamodb').Table(args.database_name)
    records = get_records(dynamodb_resource)

    # Get list of attribute names
    attrs = set()
    for record in records:
        attrs.update(record.keys())
    attrs = sorted(attrs)

    # Output record data
    print(*attrs, sep='\t')
    for record in records:
        data = [record.get(attr, '-') for attr in attrs]
        print(*data, sep='\t')


def get_records(dynamodb_resource):
    records = list()
    response = dynamodb_resource.scan()
    if 'Items' not in response:
        message = f'error: could not any records using partition key ({submission_prefix})'
        print(message, fh=sys.stderr)
        sys.exit(1)
    else:
        records.extend(response.get('Items'))
    while last_result_key := response.get('LastEvaluatedKey'):
        response = dynamodb_resource.scan(
            ExclusiveStartKey=last_result_key,
        )
        records.extend(response.get('Items'))
    return records


if __name__ == '__main__':
    main()
