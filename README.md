# agha-data-validation-scripts


This repository is a supplementary of [ummcr/agha-data-validation-pipeline](https://github.com/umccr/agha-data-validation-pipeline).
Wrapping script into images and pushed to ECR.

The repository has a codebuild to build images from python scripts located in `/assets` and will detect changes to the 
file and push image accordingly. Name, version-tag are configured through the CDK app at [buildspec environment variable](/stacks/codebuild_stack.py#L45).

## Build Docker Image manually
___

### Create virtual environment
```bash
python3 -m venv .venv/
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