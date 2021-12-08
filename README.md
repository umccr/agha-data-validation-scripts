# agha-data-validation-scripts


This repository is a supplementary of ummcr/agha-data-validation-pipeline. This repository will provide script wrapped
into images and pushed to ecr.  

The repository has a codebuild to build images from python scripts located in `/assets` and will detect changes to the 
file and push image accordingly. Tag versioning is configured through the CDK app.

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