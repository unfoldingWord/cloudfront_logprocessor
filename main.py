import os
import gzip
import csv
from dotenv import load_dotenv
import datetime
import time
import tracemalloc
import boto3
import graphyte
import logging
import json
import requests
import re


class CloudFrontLogProcessor:
    def __init__(self):
        load_dotenv()

        self.logger = logging.getLogger("cf_logger")
        if os.getenv("STAGE") == "dev":
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        # S3 connection setup
        aws_session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_ACCESS_KEY_SECRET")
        )

        self.S3_LOG_BUCKET = os.getenv("AWS_LOG_BUCKET")
        self.s3_connection = aws_session.resource("s3")
        self.s3_log_bucket = self.s3_connection.Bucket(self.S3_LOG_BUCKET)

        self.loki_api_path = os.getenv("LOKI_API_PATH")
        self.max_files = int(os.getenv("MAX_FILES"))
        self.max_lines = int(os.getenv("MAX_LINES"))

        # Constants
        self.METRIC_FILES_PROCESSED = "files-processed"
        self.METRIC_LINES_PROCESSED = "lines-processed"
        self.METRIC_FAILURE_IN_BATCH = "failure-in-batch"
        self.METRIC_MEMORY_USAGE_MIN = "memory-usage-min"
        self.METRIC_MEMORY_USAGE_MAX = "memory-usage-max"
        self.METRIC_TIME_ELAPSED = "time_elapsed"

        self.dict_metrics = self.init_metrics()

    def init_metrics(self):
        metrics = dict()
        metrics[self.METRIC_FILES_PROCESSED] = 0
        metrics[self.METRIC_LINES_PROCESSED] = 0
        metrics[self.METRIC_FAILURE_IN_BATCH] = 0
        metrics[self.METRIC_MEMORY_USAGE_MIN] = 0
        metrics[self.METRIC_MEMORY_USAGE_MAX] = 0
        metrics[self.METRIC_TIME_ELAPSED] = 0

        return metrics

    def inc_metric(self, metric, incr=1):
        self.dict_metrics[metric] += incr

    def set_metric(self, metric, value):
        self.dict_metrics[metric] = value

    def get_metrics(self):
        return self.dict_metrics

    def get_log_files(self):
        lst_objects = list()

        # List objects within this bucket
        max_iter = self.max_files

        current_iter = 0
        for obj in self.s3_log_bucket.objects.all():
            lst_objects.append(obj.key)

            current_iter += 1

            if current_iter == int(max_iter):
                break

        return lst_objects

    def read_log_file(self, distribution, filepath):
        # Gunzip
        with gzip.open(filepath, 'rt') as logfile:
            # Skip first two lines, as they contain the version resp. the headers
            next(logfile)
            next(logfile)

            # We can treat the log file as CSV
            reader = csv.reader(logfile, delimiter='\t')

            lst_loglines = []
            for row in reader:
                lst_loglines.append([distribution, row])

            self.inc_metric(self.METRIC_LINES_PROCESSED, len(lst_loglines))
            self.inc_metric(self.METRIC_FILES_PROCESSED)

            self.logger.debug("File %s contains %d lines" % (filepath, len(lst_loglines)))

            return lst_loglines

    def remove_log_file(self, obj_s3_file):
        # Only when running on prod, remove files
        if os.getenv("STAGE") == 'prod':
            self.logger.debug("Removing file %s" % obj_s3_file.key)
            obj_s3_file.delete()

    def build_loki_lines(self, lst_lines):
        dict_loki_lines = dict()

        for item in lst_lines:
            # First element is distribution, needed for stream/label
            distribution = item[0]
            # Second element is actual line
            line = item[1]

            # year, needed for stream/label
            year = line[0].split("-")[0]

            if distribution not in dict_loki_lines:
                dict_loki_lines[distribution] = dict()

            if year not in dict_loki_lines[distribution]:
                dict_loki_lines[distribution][year] = list()

            # create timestamp (unix epoch in nanoseconds)
            ts = datetime.datetime.strptime(line[0] + " " + line[1], "%Y-%m-%d %H:%M:%S").timestamp()
            # timestamp() returns float, so we convert to int to get rid of the '.0'
            ts = str(int(ts)) + "000000000"

            # build the line and add to dictionary
            loki_line = "\t".join(line)
            dict_loki_lines[distribution][year].append({"ts": ts, "line": loki_line})

        # By sorting lines by timestamp, we make sure that
        # at least within each batch the lines are being sent
        # in the correct order. This prevents almost all 'entry too far behind' issues.
        for distribution in dict_loki_lines:
            for year in dict_loki_lines[distribution]:
                dict_loki_lines[distribution][year].sort(key=lambda item: item.get("ts"))

        return dict_loki_lines

    def send_to_loki(self, dict_lines):
        url = self.loki_api_path
        headers = {
            'Content-type': 'application/json'
        }

        payload = {"streams": list()}

        total_lines = 0
        for distribution in dict_lines:
            for year in dict_lines[distribution]:

                labels = {"system": "cloudfront", "distribution": distribution, "year": year}

                lst_values = list()
                for entry in dict_lines[distribution][year]:
                    total_lines += 1
                    lst_values.append([entry["ts"], entry["line"]])

                payload["streams"].append({"stream": labels, "values": lst_values})

        # print(json.dumps(payload, indent=2))

        payload = json.dumps(payload)
        try:
            answer = requests.post(url, data=payload, headers=headers)

            if answer.status_code != 204:
                self.set_metric(self.METRIC_FAILURE_IN_BATCH, 1)
                self.set_metric("failure.status-code." + str(answer.status_code), 1)

                if answer.status_code == 400:
                    # 400 - Bad Request:
                    #   - Entries out of order;
                    #   - Timestamp too new
                    # Find the amount of ignored lines
                    regex = "total ignored: ([0-9]+) out of"
                    match = re.search(regex, str(answer.content))
                    if len(match.group()) > 0:
                        self.set_metric("lines-failed", int(match.group(1)))

                if answer.status_code in [429, 502, 500]:
                    # 429 - Too many requests: Ingestion rate limit exceeded
                    # 500 - Internal server error:
                    #   - RPC received message larger than max
                    # 502 - Bad gateway: Loki has issues
                    return False

                self.logger.debug(str(answer.status_code) + str(answer.content))

        except requests.exceptions.ConnectionError:
            # In case we cannot even connect
            return False

        # By default, all is fine ;-)
        # Sleep a sec, to give Loki some time to breathe
        self.logger.debug("Succesfully sent " + str(total_lines) + " lines")
        time.sleep(1)
        return True

    def send_metrics(self):
        graphite_host = os.getenv("GRAPHITE_HOST")
        graphite_prefix = os.getenv("GRAPHITE_PREFIX")

        graphyte.init(graphite_host, prefix=graphite_prefix)

        dict_metrics = self.get_metrics()
        for key in dict_metrics:
            graphyte.send(key, dict_metrics[key])

    def run(self):
        tracemalloc.start()
        time_start = time.perf_counter()

        lst_files = self.get_log_files()

        lst_log_lines = list()
        lst_s3_files = list()

        for file in lst_files:

            self.logger.debug("Processing " + file)

            # The Cloudfront 'distribution' is the first part of the filename
            distribution = file.split("/")[0]

            # Download S3 file to temp location
            tmp_file = "/tmp/" + file.replace("/", "_")
            obj_s3_file = self.s3_connection.Object(bucket_name=self.S3_LOG_BUCKET, key=file)
            obj_s3_file.download_file(tmp_file)

            # Get the actual data
            lst_log_lines += self.read_log_file(distribution, tmp_file)

            # Add s3 file to list
            lst_s3_files.append(obj_s3_file)

            # Remove tmp_file
            os.remove(tmp_file)

        # There are cases when we still go WAY over the maximum number of lines allowed.
        # This can cause a data overload on the server side, resulting in an HTTP 500 error.
        # To prevent this, we chunk up our lines in neat batches of max_lines
        lst_chunked = [lst_log_lines[i:i + self.max_lines] for i in range(0, len(lst_log_lines), self.max_lines)]

        send_to_loki_succeeded = True
        for lst_sublist in lst_chunked:
            # fold the log lines into a usable structure
            dict_log_lines = self.build_loki_lines(lst_sublist)

            # Send files to loki
            if not self.send_to_loki(dict_log_lines):
                send_to_loki_succeeded = False

        # Remove files from S3 storage
        # Only if all send_to_loki operations succeeded (if just only one fails, none of the files will be removed)
        if send_to_loki_succeeded:
            for s3_file in lst_s3_files:
                self.remove_log_file(s3_file)

        # Finally, collect performance metrics
        time_end = time.perf_counter()
        mem_usage = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        self.set_metric(self.METRIC_TIME_ELAPSED, time_end - time_start)
        self.set_metric(self.METRIC_MEMORY_USAGE_MIN, mem_usage[0])
        self.set_metric(self.METRIC_MEMORY_USAGE_MAX, mem_usage[1])

        # Log metrics to screen, for easy reference later on
        self.logger.info(self.get_metrics())

        # Finish off by sending all collected metrics to graphite
        self.send_metrics()


# Don't remove the next empty logging statement.
# I need this, otherwise logging doesn't start at all. Bug or my stupidity?
logging.info("")

obj_CFProc = CloudFrontLogProcessor()
obj_CFProc.run()
