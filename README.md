# AWS CloudFront Standard Log Processor

With this script, you can process the Standard Log files from AWS

## Description

With AWS CloudFront, you can either send your log files to CloudWatch, 
or you can have them stored as Standard Log files in an S3 bucket. 
This script takes care of these log files by
1) Reading logfiles from a log bucket
2) Sending the lines from these log files to a MySQL compatible DB
3) Removing each logfile after processing
4) Sending metrics to a graphite server 

## Getting Started

### Dependencies

For Python dependencies, see `requirements.txt`.

You also need to install a graphite server and a MySQL DB. 
For easy testing and/or deployment, use the following docker containers:
- [graphiteapp/graphite-statsd](https://hub.docker.com/r/graphiteapp/graphite-statsd)
- [mariadb](https://hub.docker.com/_/mariadb)

You need to create a database first. After that, you can create the needed 
table with the help of the provided `setup_db.sql`.

### Installing

- Clone this repository
```
git clone git@github.com:unfoldingWord-dev/cloudfront_logprocessor
cd 
pip install -r requirements.txt
```

- Or pull the docker container from [here](https://hub.docker.com/r/unfoldingword/cloudfront_logprocessor)
```
docker pull unfoldingword/cloudfront_logprocessor
```

- Or build your own docker container with the help of the provided Dockerfile
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

- `DB_HOST` (e.g. `127.0.0.1`)
- `DB_USER` (e.g. `root`)
- `DB_PASSWORD` (e.g. `secretpassword`)
- `DB_DATABASE` (e.g. `db_log`)
- `AWS_ACCESS_KEY_ID`
- `AWS_ACCESS_KEY_SECRET`
- `AWS_LOG_BUCKET` (e.g. `logs.acme.org`)
- `GRAPHITE_HOST` (e.g. `localhost`)
- `GRAPHITE_PREFIX` (e.g. `aws.lambda.cloudfront_logprocessor`)
- `MAX_FILES` (e.g. `1000`)
- `STAGE` (`dev` or `prod`)

## Authors

- [yakob-aleksandrovich ](https://github.com/yakob-aleksandrovich)

## Version History

* 0.1
    * Initial Release

## License

This project is licensed under the MIT License