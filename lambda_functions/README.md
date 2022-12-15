# Lambda functions

## Check S3 Files
Function created to ensure the reliability of the pipeline. The process won't execute until all necessary files are in the S3 bucket.

## Start Crawler
Function responsible for invoking a specific crawler.

## Check Crawler Status
Function ensuring the reliability of the pipeline. The process won't continue if the crawler run hasn't finished.

## Reports
Functions created to generate reports.

## Send email
Simple function to send email notification that the process has finished.

## Send email with attachment
Function to send email with reports attached.