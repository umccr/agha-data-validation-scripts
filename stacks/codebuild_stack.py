from aws_cdk import (
    core,
    aws_ecr as ecr,
    aws_codebuild as codebuild
)


class CodebuildStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ################################################################################
        # ECR
        validate_file_ecr_repo = ecr.Repository(
            self,
            "AGHAValidationEcrRepo",
            image_tag_mutability=ecr.TagMutability.MUTABLE,
            removal_policy=core.RemovalPolicy.DESTROY,
            repository_name="agha-gdr-validate-file",
            lifecycle_rules=[
                ecr.LifecycleRule(
                    description="Remove untagged images if exist more than 1.",
                    rule_priority=1,
                    max_image_count=1,
                    tag_status=ecr.TagStatus.UNTAGGED
                )
            ]
        )

        ################################################################################
        # Codebuild Project

        codebuild_build_image = codebuild.Project(
            self,
            "CodebuildProjectAGHAValidationImage",
            source=codebuild.Source.git_hub(
                owner="umccr",
                repo="agha-data-validation-scripts",
                webhook=True,
                webhook_filters=[
                    codebuild.FilterGroup.in_event_of(codebuild.EventAction.PUSH).and_branch_is("main")
                        .and_file_path_is("assets\/validate_file\.py"),
                    codebuild.FilterGroup.in_event_of(codebuild.EventAction.PUSH).and_branch_is("main")
                        .and_file_path_is("assets\/Dockerfile")
                ]
            ),
            project_name="agha_validation_image_build",
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0,
                privileged=True
            ),
            environment_variables={
                "NAME": codebuild.BuildEnvironmentVariable(
                    value=validate_file_ecr_repo.repository_name,
                    type=codebuild.BuildEnvironmentVariableType.PLAINTEXT
                ),
                "VERSION": codebuild.BuildEnvironmentVariable(
                    value="0.0.1",
                    type=codebuild.BuildEnvironmentVariableType.PLAINTEXT
                ),
                "AWS_PROVIDER_URI": codebuild.BuildEnvironmentVariable(
                    value=validate_file_ecr_repo.repository_uri,
                    type=codebuild.BuildEnvironmentVariableType.PLAINTEXT
                ),

            },
            build_spec=codebuild.BuildSpec.from_object({
                "version": "0.2",
                "phases": {
                    "pre_build": {
                        "commands": [
                            "aws ecr get-login-password --region ap-southeast-2 | "
                            "docker login --username AWS --password-stdin ${AWS_PROVIDER_URI}"
                        ]
                    },
                    "build": {
                        "commands": ["docker build -t ${NAME} -f assets/Dockerfile .",
                                     "docker tag ${NAME} ${AWS_PROVIDER_URI}:${VERSION}",
                                     "docker push ${AWS_PROVIDER_URI}:${VERSION}"]
                    }
                }
            })
        )

        validate_file_ecr_repo.grant_pull_push(codebuild_build_image)

        # TODO: Might put this as a for loop when multiple script need to be published from assets directory to ECR
