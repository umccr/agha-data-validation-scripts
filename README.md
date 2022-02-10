# agha-data-validation-scripts


This repository is a supplementary of [ummcr/agha-data-validation-pipeline](https://github.com/umccr/agha-data-validation-pipeline).
Wrapping script into images and pushed to ECR.

The repository has a codebuild to build images from python scripts located in `/assets` and will detect changes to the 
file and push image accordingly. Name, version-tag are configured through the CDK app at [buildspec environment variable](/stacks/codebuild_stack.py#L45).

## Build Docker Image manually
___

### Create virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Build Docker image
Configure
```bash
NAME=agha-gdr-file-validation
VERSION=0.0.1
```

Build
```bash
docker build -t "${NAME}" -f assets/Dockerfile .
```
## Developing manually
___

The script on the assets folder most likely use utils from [ummcr/agha-data-validation-pipeline](https://github.com/umccr/agha-data-validation-pipeline/tree/dev/lambdas/layers/util).
The Dockerfile will download and copy its as part of the container. If you want to copy `/util/` file in the local directory, use following command.

Install subversion
```bash
sudo apt-get update
sudo apt-get install subversion
```

Copy the file
```bash
svn checkout https://github.com/umccr/agha-data-validation-pipeline/branches/dev/lambdas/layers/util
```

To Run the file manually run the docker run command. 
The following command is an example how to run python script.
```bash
docker run \
--env RESULTS_BUCKET=staging_bucket \
--env STAGING_BUCKET=store_bucket \
--env RESULTS_KEY_PREFIX=ACG/20210722_090101 \
--env AWS_BATCH_JOB_ID=007 \
--env AWS_ACCESS_KEY_ID=XXX \
--env AWS_SECRET_ACCESS_KEY=XXX \
--env AWS_SESSION_TOKEN=XXX \
${NAME} \
--s3_key ACG/20210722_090101/short_reads_1.fastq \
--tasks FILE_VALIDATION CREATE_INDEX CHECKSUM_VALIDATION CREATE_COMPRESS \
--checksum 	b8f767ef5a9986bf602e51f4aacbc2b2

```