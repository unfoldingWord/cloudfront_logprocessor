import mysql.connector
import os
import gzip
import csv
from dotenv import load_dotenv
import time
import tracemalloc
import boto3
import graphyte
import logging


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

        # Constants
        self.METRIC_FILES_PROCESSED = "files-processed"
        self.METRIC_LINES_PROCESSED = "lines-processed"
        self.METRIC_LINES_INSERTED = "lines-inserted"
        self.METRIC_MEMORY_USAGE_MIN = "memory-usage-min"
        self.METRIC_MEMORY_USAGE_MAX = "memory-usage-max"
        self.METRIC_TIME_ELAPSED = "time_elapsed"

        self.db = self.connect()

        self.dict_metrics = self.init_metrics()

    def init_metrics(self):
        metrics = dict()
        metrics[self.METRIC_FILES_PROCESSED] = 0
        metrics[self.METRIC_LINES_PROCESSED] = 0
        metrics[self.METRIC_LINES_INSERTED] = 0
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

    def connect(self):
        my_db = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_DATABASE"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )

        return my_db

    def get_log_files(self):
        lst_objects = list()

        # List objects within this bucket
        max_iter = os.getenv("MAX_FILES")

        current_iter = 0
        for obj in self.s3_log_bucket.objects.all():
            lst_objects.append(obj.key)

            current_iter += 1

            if current_iter == int(max_iter):
                break

        return lst_objects

    def read_log_file(self, filepath):
        # Gunzip
        with gzip.open(filepath, 'rt') as logfile:
            # Skip first two lines, as they contain the version resp. the headers
            next(logfile)
            next(logfile)

            # We can treat the log file as CSV
            reader = csv.reader(logfile, delimiter='\t')

            lst_loglines = []
            for row in reader:
                # Replace dashes with None values
                row = [None if col == "-" else col for col in row]

                lst_loglines.append(row)

            self.inc_metric(self.METRIC_LINES_PROCESSED, len(lst_loglines))
            self.inc_metric(self.METRIC_FILES_PROCESSED)

            self.logger.debug("File %s contains %d lines" % (filepath, len(lst_loglines)))

            return lst_loglines

    def remove_log_file(self, obj_s3_file):
        # Only on prod, remove files
        if os.getenv("STAGE") == 'prod':
            self.logger.debug("Removing file %s" % obj_s3_file.key)
            obj_s3_file.delete()

    def get_log_columns(self, filepath):
        with gzip.open(filepath, 'rt') as logfile:
            # Skip first line, contains version number
            next(logfile)

            # Second line contains headers, but we need to remove the prefix
            header = logfile.readline()
            lst_header = header.replace("#Fields: ", "").split(" ")

            # Linebreaks may be in the file, so we strip them out
            lst_header = [header.rstrip("\n") for header in lst_header]

            return lst_header

    def insert_into_db(self, lst_columns, lst_loglines):

        sql_columns = "`" + "`, `".join(lst_columns) + "`"
        sql_placeholders = ", ".join(["%s" for elm in lst_columns])

        sql_prepared = "INSERT INTO `cloudfront_log` (" + sql_columns + ") VALUES (" + sql_placeholders + ")"

        try:
            cursor = self.db.cursor()
            cursor.executemany(sql_prepared, lst_loglines)

            amt_inserted = cursor.rowcount
            self.inc_metric(self.METRIC_LINES_INSERTED, amt_inserted)

            self.db.commit()

            if int(amt_inserted) == len(lst_loglines):
                return True

            else:
                amt_skipped = len(lst_loglines) - int(amt_inserted)

                self.logger.error("%s out of %s lines have not been inserted" % (amt_skipped, len(lst_loglines)))

        except mysql.connector.Error as sql_error:
            self.logger.error(sql_error)

        # We will only arrive here when something went wrong
        return False

    def collect_db_metrics(self):
        cursor = self.db.cursor()

        # Total number of lines in DB
        sql_count_total = "SELECT COUNT(*) FROM `cloudfront_log`"
        cursor.execute(sql_count_total)

        result = cursor.fetchone()
        self.set_metric("db.lines-total", result[0])

        # Number of lines per host
        sql_count_per_host = "SELECT `x-host-header`, COUNT(*) FROM `cloudfront_log` GROUP BY `x-host-header`"
        cursor.execute(sql_count_per_host)

        result = cursor.fetchall()
        for item in result:
            host = str(item[0]).replace(".", "_")

            self.set_metric("db.lines-per-host." + host, item[1])

    def send_metrics(self):
        graphite_host = os.getenv("GRAPHITE_HOST")
        graphite_prefix = os.getenv("GRAPHITE_PREFIX")

        graphyte.init(graphite_host, prefix=graphite_prefix)

        dict_metrics = self.get_metrics()
        for key in dict_metrics:
            graphyte.send(key, dict_metrics[key])

    def run(self):
        lst_files = self.get_log_files()

        tracemalloc.start()
        time_start = time.perf_counter()

        for file in lst_files:
            # Download S3 file to temp location
            tmp_file = "/tmp/" + file.replace("/", "_")

            obj_s3_file = self.s3_connection.Object(bucket_name=self.S3_LOG_BUCKET, key=file)
            obj_s3_file.download_file(tmp_file)

            # Get the headers
            lst_columns = self.get_log_columns(tmp_file)

            # Get the actual data
            lst_loglines = self.read_log_file(tmp_file)

            # Insert into DB
            # Only when we have inserted everything, we can remove the file
            if self.insert_into_db(lst_columns, lst_loglines):
                self.remove_log_file(obj_s3_file)

            # Remove tmp_file
            os.remove(tmp_file)

        # Collect metrics about DB
        self.collect_db_metrics()

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
