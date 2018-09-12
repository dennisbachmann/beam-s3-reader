# Apache Beam - S3 Reader POC

This project is a proof-of-concept (POC) of an Apache Beam pipeline that reads files from a S3 bucket and outputs their contents using a logger.

Beam version: `2.6.0`

Main class:
`bachmann.example.beam.ReadFromAmazonS3Pipeline`

Parameters:
```
--awsCredentialsProvider="{\"@type\" : \"AWSStaticCredentialsProvider\", \"awsAccessKeyId\" : \"AWS_ACCESS_KEY\", \"awsSecretKey\" : \"AWS_SECRET_KEY\"}"
--awsRegion="AWS_REGION i.e. us-east-1"
--s3Bucket="BUCKET_NAME"
--s3FilePattern="FILE_PATTERN"
```
