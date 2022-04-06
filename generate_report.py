import logging
import os
from datetime import datetime as dt
import csv
import pandas
import argparse
import pyodbc
import configparser

from sqlalchemy import create_engine
from sqlalchemy.engine import URL


def db_connection(conn_params):
    log = logging.getLogger(__name__)
    try:
        log.info("Trying to connect to database {0} using user {1}".format(conn_params["database"],
                                                                           conn_params["user"]))
        driver = conn_params['driver']
        host = conn_params['host']
        database = conn_params['database']
        uid = conn_params['user']
        password = conn_params['pwd']
        conn_string = """Driver={%s}; Server=%s; Database=%s;
                                            UID=%s; PWD=%s""" % (driver, host, database, uid, password)

        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_string})
        log.debug(conn_string)
        engine = create_engine(connection_url)
        log.info("Connection successful.")
        return engine
    except Exception as e:
        log.error(e)


def get_config(filename):
    cf = configparser.ConfigParser()
    cf.read(filename)  # Read configuration file
    return cf


def main():
    log = logging.getLogger(__name__)
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--ini_file", required=True,
                            help="location of db config json file")

    group = arg_parser.add_mutually_exclusive_group(required=True)

    group.add_argument("--sql_file", help="Location of the SQL File we want to process")
    group.add_argument("--sql_query", help="Actual SQL query to execute")

    arg_parser.add_argument("--output_file", required=True,
                            help="File to dump the results")
    arg_parser.add_argument("--delimiter", default=",",
                            help=("Any specific delimiter we " +
                                  "want to split the columns by"))
    arg_parser.add_argument("--log_level", help="level of logging",
                            default="INFO")
    arg_parser.add_argument("--process_date",
                            help="Date to process for, if required",
                            default=dt.now().strftime("%Y-%m-%d"))
    arg_parser.add_argument("--include_header", default=False,
                            action='store_true',
                            help=" pass this to include header.")

    arg_parser.add_argument("--replace_label_values",
                            help=("Pipe Delimited Values which will" +
                                  " replace LABELS in your SQLs"))

    arg_parser.add_argument("--append_to_file", default=False,
                            action="store_true",
                            help="Append to existing file")

    arg_parser.add_argument('--loglevel',
                            default='info',
                            help='Provide logging level. Example --loglevel debug, default=info')

    options = arg_parser.parse_args()
    logging.basicConfig(level=options.loglevel.upper(),
                        format='%(asctime)s : %(name)s - %(levelname)s '
                               '- %(message)s', handlers=[
            logging.FileHandler("generate_report.log"),
            logging.StreamHandler()]
                        )
    log.info("-------------------- Report generation started --------------------")

    conf = get_config(options.ini_file)
    engine = db_connection(conf["Database"])

    try:
        if options.sql_query is None:
            # Reading sql file
            log.info("Opening file %s to read query.", options.sql_file)
            fh = open(options.sql_file, 'r')
            sql_query = fh.read()
            fh.close()
        else:
            sql_query = options.sql_query

        log.info("Replacing DATE parameters if any with process date {0}".format(options.process_date))
        sql_query = sql_query.replace("{DATE}", options.process_date)

        # We will now replace labels- if they are supplied any by the user In SQL file you must add label's as {
        # LABEL_1},{LABEL_2} ... the parameters passed will be the actual column names you want(add them as pipe
        # delimited)- "Column1|Column2.."

        if options.replace_label_values is not None:
            log.info("Labels have been passed! ")
            values = options.replace_label_values.split("|")
            for index, str_val in enumerate(values):
                replace_me = "{LABEL_" + str(index) + "}"
                sql_query = sql_query.replace(replace_me, str_val)

        log.debug("QUERY: %s", sql_query)

        log.info("...using pandas to do csv")
        df = pandas.read_sql(sql=sql_query, con=engine)
        if options.append_to_file:
            mode = 'a'
        else:
            mode = 'w'
        df.to_csv(options.output_file, index=False, sep=options.delimiter,
                  encoding='utf-8', header=options.include_header, quoting=csv.QUOTE_NONNUMERIC, mode=mode
                  )

        log.info("Process completed...exiting")
    except Exception as e:
        log.error(e)


if __name__ == "__main__":
    main()
