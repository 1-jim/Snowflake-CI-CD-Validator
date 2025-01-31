# Snowflake CI-CD Validator

This python code is used to crawl a Snowflake scripts directory for .sql files and execute the statements within an 'EXPLAIN' function within Snowflake. This function will produce an execution plan if it is valid - therefore enabling us to identify invalid CREATE statements prior to deployment, minimising failed releases and avoiding lost time.

> This was originally developed to integrate with SchemaChange and Azure DevOps. I have since replaced the 3rd party dependency on SchemaChange with my own code now that Snowflake has released native Git integration and now that Python Snowpark connections can securely use Public/Private keypair authentication.

As an example use case, this script can run prior to creation of the build file on the Development instance, to halt any release or promotion until the errors are fixed. Within the failed build, you will be provided with verbose error reporting of all failing SQL statements, not just the first failure. Prior to introducing this code, SchemaChange would fail on the first problem encountered in the release, with no understanding of how many other issues are lurking.

## Any change to what I do now?

Not really, this is just a gate for the DEV branch, to ensure that the script you have added can be EXPLAINed by Snowflake.
> The only thing to be wary of is if you create a new table and create a reference to it within the same commit. Your commit will fail validation as the table has not been created yet. If this happens, just create the table manually in DEV and re-run the build. Alternatively, if there are too many code changes to handle manually - comment out the validator part of the pipeline, then run the pipe just to get the code in, then reinstate.

## How do I run locally?

You just need to get hold of the Snowflake keys and create the environment variables as per the pipeline e.g.

```text
PRIVATE_KEY_PASSPHRASE=<SECRET>
RSA_KEY_PATH=./COMPANY_dev_rsa_key.p8
ENVIRONMENT=DEV
SF_ACCOUNT=SOMETHING.uk-south.azure
SF_USER=COMPANY_ETL_DEV
SF_WAREHOUSE=WH_COMPANY_DEV
SF_ROLE=COMPANY_ETL_ROLE
SF_DATABASE=COMPANY_DEV
SF_SCHEMA=TEST_SCHEMA
SF_CODE_FOLDER=../snowflake/
```

> note that the SF_CODE_FOLDER is different to the default - as the default bundle in DevOps copies the files to the same dir as this code, whereas in the Repo, this code is in a subfolder.

## How do we do this then?

The build pipeline will require the following two tasks added to an existing build yaml, prior to the Archive and Build steps.

The SNOWFLAKE_SCHEMA is all you need to set, as this is where you set your target folder for validation.

> Environment Variables will originate from the variable group being shared with the Pipeline - with the exception of SNOWFLAKE_SCHEMA

This is the example script to be added to an existing dev build pipeline;

```yaml
- task: Bash@3
  displayName: 'install prerequisites'
  inputs:
    targetType: 'inline'
    script: 'pip install -r ./Snowflake/_Tenantsetup/validator/explain_validator_requirements.txt'

- task: PythonScript@0
  displayName: 'run Snowflake EXPLAIN tests'
  env:
    SNOWFLAKE_SCHEMA: MY_SCHEMA
    SNOWFLAKE_USER: $(SF_USER)
    SNOWFLAKE_PASSWORD: $(SF_PASSWORD)
    SNOWFLAKE_ACCOUNT: $(SF_ACCOUNT)
    SNOWFLAKE_WAREHOUSE: $(SF_WAREHOUSE)
    SNOWFLAKE_DATABASE: $(SF_DATABASE)
  inputs: 
    scriptSource: filePath
    scriptPath: './Snowflake/_Tenantsetup/validator/explain_validator_v1.py'
    failOnStderr: true
```

This is an example complete build pipeline if you are creating the dev build pipeline from scratch;

```yaml
# snowflake build pipeline verfify using EXPLAIN
trigger:
  branches:
    include:
    - dev
  paths:
    include:
    - Snowflake/COMPANY_PROD/MY_SCHEMA

pool:
  vmImage: 'ubuntu-latest'

steps:
- task: Bash@3
  displayName: 'install prerequisites'
  inputs:
    targetType: 'inline'
    script: 'pip install -r ./Snowflake/_Tenantsetup/validator/explain_validator_requirements.txt'

- task: PythonScript@0
  displayName: 'run Snowflake EXPLAIN tests'
  env:
    SNOWFLAKE_SCHEMA: MY_SCHEMA
    SNOWFLAKE_USER: $(SF_USER)
    SNOWFLAKE_PASSWORD: $(SF_PASSWORD)
    SNOWFLAKE_ACCOUNT: $(SF_ACCOUNT)
    SNOWFLAKE_WAREHOUSE: $(SF_WAREHOUSE)
    SNOWFLAKE_DATABASE: $(SF_DATABASE)
  inputs: 
    scriptSource: filePath
    scriptPath: './Snowflake/_Tenantsetup/validator/explain_validator_v1.py'
    failOnStderr: true

- task: ArchiveFiles@2
  inputs:
    rootFolderOrFile: 'Snowflake/COMPANY_PROD/MY_SCHEMA'
    includeRootFolder: true
    archiveType: 'zip'
    archiveFile: '$(Build.ArtifactStagingDirectory)/$(Build.BuildId).zip'
    replaceExistingArchive: true

- task: PublishBuildArtifacts@1
  inputs:
    PathtoPublish: '$(Build.ArtifactStagingDirectory)'
    ArtifactName: 'MY_SCHEMA'
    publishLocation: 'Container'
  ```
  
