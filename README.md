# AWS CloudFront Standard Log Processor

Import Standard Log files from AWS into Loki

## Description

With AWS CloudFront, you can either send your log files to CloudWatch, 
or you can have them stored as Standard Log files in an S3 bucket. 
This script takes care of these log files by
1) Reading logfiles from a log bucket
2) Sending the lines from these log files to a Loki instance
3) Removing each logfile after processing
4) Sending metrics to a graphite server 

## Getting Started

### Dependencies

For Python dependencies, see `requirements.txt`.

You also need to setup a graphite server and a Loki server
For easy testing and/or deployment, use the following docker containers:
- [graphiteapp/graphite-statsd](https://hub.docker.com/r/graphiteapp/graphite-statsd)
- [grafana/loki](https://hub.docker.com/r/grafana/loki)


### Installing

- Clone this repository
```
git clone git@github.com:unfoldingWord/cloudfront_logprocessor
cd cloudfront_logprocessor
pip install -r requirements.txt
```

- Or pull the docker image from [here](https://hub.docker.com/r/unfoldingword/cloudfront_logprocessor)
```
docker pull unfoldingword/cloudfront_logprocessor
```

- Or build your own docker image with the help of the provided Dockerfile
```
docker build -t <dockerhub-username>/<repo-name> .
```

### Executing program
#### Running the python program
```
python path/to/repository/main.py
```

#### Running as a docker container
```
docker run --env-file .env --rm --net=host --name cf-processor unfoldingword/cloudfront_logprocessor
```

You need to provide the following environment variables, 
either through a .env file, or by setting them manually

- `AWS_ACCESS_KEY_ID` *(your AWS Access Key ID)*
- `AWS_ACCESS_KEY_SECRET` *(your AWS Access Key Secret)*
- `AWS_LOG_BUCKET` *(The bucket containing the access log files. E.g. `logs.acme.org`)*
- `GRAPHITE_HOST` *(Your graphite host, to send metrics to. E.g. `localhost`)*
- `GRAPHITE_PREFIX` *(Prefix for all graphite entries. E.g. `aws.cloudfront_logprocessor`)*
- `MAX_FILES` *(Maximum number of files to process per run. E.g. `1000`)*
- `MAX_LINES` *(Maximum number of log lines to process per run. E.g. `10000`)*
- `STAGE` (Are you running on `dev` or `prod`?)
On `dev`, we will pull in files from S3, but not remove them. Also, we are a bit more verbose with logging.

## Authors

- [yakob-aleksandrovich ](https://github.com/yakob-aleksandrovich)

## Version History

* 0.1
    * Initial Release
* 0.2
    * Log messages are written to Loki instead of Mysql DB.
    * Implemented MAX_LINES ENV variable, mostly for Loki's sake

## License

This project is licensed under the MIT License
