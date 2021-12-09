
# Build on top of Ubuntu LTS (focal)
FROM public.ecr.aws/ubuntu/ubuntu:20.04

# Set Conda environment name
ARG ENV_NAME=agha

# Install some packages through system package manager
RUN \
  apt-get update && \
  apt-get install --yes --no-install-recommends \
    ca-certificates  \
    subversion \
    curl && \
  # Clean up
  apt-get clean && \
  rm -rf /var/lib/apt/*

# Install Mambdaforge
RUN \
  curl -L 'https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh' -o 'mambaforge.sh' && \
  /bin/bash mambaforge.sh -b -p /opt/mambaforge/ && \
  rm mambaforge.sh

# Prepare Conda environment
RUN \
  eval "$(/opt/mambaforge/bin/conda shell.bash hook)" && \
  mamba create \
    --name "${ENV_NAME}" \
    --channel umccr \
    --channel bioconda \
    --channel conda-forge \
    --channel defaults \
    --yes \
    bcftools \
    boto3 \
    'umccr::fqtools' \
    samtools && \
  mamba clean --all --force-pkgs-dirs --yes

# Add Conda env to PATH
ENV PATH="/opt/mambaforge/envs/${ENV_NAME}/bin:/opt/mambdaforge/bin:${PATH}"
ENV CONDA_PREFIX="/opt/mambaforge/envs/${ENV_NAME}"

# Install 'util' Python package
RUN svn checkout https://github.com/umccr/agha-data-validation-pipeline/trunk/lambdas/layers/util /opt/util/
RUN \
    pip install /opt/util/ && \
    rm -r /opt/util/

# Pull in execution script
COPY assets/validate_file.py /opt/