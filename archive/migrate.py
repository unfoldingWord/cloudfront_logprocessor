# This script is a temporary measure.
# It migrates data from the RDS DB to Loki
import mysql.connector
import os
from dotenv import load_dotenv
import logging
import datetime, time
import json
import requests
import math


class CFMigrator:
    def __init__(self):
        load_dotenv()

        self.max_lines = os.getenv("MAX_LINES")

        self.logger = logging.getLogger("cf_logger")
        self.stage = os.getenv("STAGE")
        self.processed_lines = 0

    def db_connect(self):
        my_db = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_DATABASE"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )

        return my_db

    def is_secure_connection(self):
        db = self.db_connect()

        query = "SHOW STATUS LIKE 'Ssl_cipher'"

        try:
            cursor = db.cursor()
            cursor.execute(query)

            cipher = cursor.fetchone()[1]

            if cipher != '':
                return True

        except mysql.connector.Error as sql_error:
            self.logger.error(sql_error)

        return False

    def pull_log_lines_from_db(self, db, max_lines, offset=0):
        query = "SELECT * FROM `cloudfront_log` LIMIT " + str(offset) + ", " + max_lines

        try:
            cursor = db.cursor()
            cursor.execute(query)

            return cursor.fetchall()

        except mysql.connector.Error as sql_error:
            self.logger.error(sql_error)

        return False

    def get_total_rows(self, db):
        query = "SELECT MAX(`log_id`) FROM `cloudfront_log`"

        try:
            cursor = db.cursor()
            cursor.execute(query)

            return cursor.fetchone()[0]

        except mysql.connector.Error as sql_error:
            self.logger.error(sql_error)

        return False

    def remove_processed_lines(self, lst_ids, db):
        str_ids = ",".join(lst_ids)

        print("Batch header ID: " + lst_ids[0])

        query = "DELETE FROM `cloudfront_log` WHERE `log_id` IN (" + str_ids + ")"

        try:
            cursor = db.cursor()
            cursor.execute(query)
            #db.commit()

        except mysql.connector.Error as sql_error:
            self.logger.error(sql_error)
            return False

        return True

    def send_to_loki(self, dict_lines):
        url = 'http://localhost:3100/loki/api/v1/push'
        headers = {
            'Content-type': 'application/json'
        }

        payload = {"streams": list()}

        for app in dict_lines:
            for year in dict_lines[app]:

                labels = {"system": "cloudfront", "distribution": app, "year": year}

                lst_values = list()
                for entry in dict_lines[app][year]:
                    lst_values.append([entry["ts"], entry["line"]])

                payload["streams"].append({"stream": labels, "values": lst_values})

        #print(json.dumps(payload, indent=2))

        payload = json.dumps(payload)
        answer = requests.post(url, data=payload, headers=headers)
        #print(answer)

        if answer.status_code != 204:
            print(answer.status_code)
            print(answer.content)
            return False

        return True

    def build_loki_lines(self, lst_lines):
        dict_loki_lines = dict()

        for line in lst_lines:
            # streams/labels: distribution (formerly app) and year
            app = line[16]
            year = line[1].year

            if app not in dict_loki_lines:
                dict_loki_lines[app] = dict()

            if year not in dict_loki_lines[app]:
                dict_loki_lines[app][year] = list()

            # create timestamp, (unix epoch in nanoseconds)
            ts = str(int((datetime.datetime.combine(line[1], datetime.time()) + line[2]).timestamp())) + "000000000"

            # line with actual information
            my_line = [str(x) for x in line[1:]]
            loki_line = "\t".join(my_line)
            loki_line = loki_line.replace("None", "-")

            dict_loki_lines[app][year].append({"ts": ts, "line": loki_line})

        # By sorting lines by timestamp, we make sure that
        # at least within each batch the lines are being sent
        # in the correct order. This prevents almost all 'entry too far behind' issues.
        for app in dict_loki_lines:
            for year in dict_loki_lines[app]:
                dict_loki_lines[app][year].sort(key=lambda item: item.get("ts"))

        return dict_loki_lines

    def migrate(self):
        if self.is_secure_connection() is False:
            print("The DB connection is NOT secure!")
            exit(1)

        start_time = datetime.datetime.now()
        print("Start: " + str(start_time))

        # Connect to DB
        db = self.db_connect()

        total_rows = self.get_total_rows(db)
        if total_rows is not False:
            total_batches = math.ceil(total_rows / int(self.max_lines))

            for batch in range(total_batches):
                offset = batch * int(self.max_lines)
                print("Batch " + str(batch + 1) + "/" + str(total_batches) + " (" + self.max_lines + " lines per batch) ")

                # pull rows from DB (we automatically get the oldest)
                if self.stage == 'dev':
                    lst_rows = self.pull_log_lines_from_db(db, self.max_lines, offset=offset)
                else:  # prod
                    lst_rows = self.pull_log_lines_from_db(db, self.max_lines)

                # fold the log lines into a usable structure
                dict_log_lines = self.build_loki_lines(lst_rows)
                #print(json.dumps(dict_log_lines, indent=2))

                # Send log lines at once to Loki
                result = self.send_to_loki(dict_log_lines)
                #if result is False:
                #    exit(1)

                # Prod-only: Remove loglines from DB
                # Dev-only: Store offset (in file?) (or just memory)?
                if self.stage == 'prod':
                    lst_ids = [str(line[0]) for line in lst_rows]
                    self.remove_processed_lines(lst_ids, db)

                print("    Time: " + str(datetime.datetime.now()))
                time.sleep(1)

                # Keep count of total processed lines
                self.processed_lines += len(lst_rows)

        end_time = datetime.datetime.now()
        print("Processed lines: " + str(self.processed_lines))
        print("End: " + str(end_time))
        print("Elapsed: " + str((end_time.timestamp() - start_time.timestamp()) / 60) + " minutes")


obj_cfmigrator = CFMigrator()
obj_cfmigrator.migrate()
