#!/usr/bin/env python3

from aws_cdk import core

from stacks.codebuild_stack import CodebuildStack

# Construct full set of properties for stack
stack_props = {
    'namespace': 'agha-data-validation-scripts-codepipeline',
    'pipeline' : {
        'artifact_bucket_name': 'agha-validation-pipeline-artifact',
        'pipeline_name': 'agha-validation-build-pipeline',
        'repository_name': 'agha-data-validation-pipeline',
        'branch_name':'dev'
    }
}

app = core.App(
    context=stack_props
)

CodebuildStack(
    app,
    "CodebuildAGHAValidationBuild",
    tags={
        "stack":stack_props["namespace"],
        "creator":f"""cdk-{stack_props["namespace"]}"""
    }
)

app.synth()
