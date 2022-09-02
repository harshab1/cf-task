import logging
import os
import sqlalchemy
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.tz import gettz

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

     # Variables - passed as Run time values while creating the Cloud SQL instance
    drivername = "mysql+pymysql"
    username = os.environ.get("username")
    password = os.environ.get("password")
    database = os.environ.get("database")
    project_id = os.environ.get("project_id")
    instance_region=os.environ.get("instance_region")
    instance_name=os.environ.get("instance_name")
    query_string = dict({"unix_socket": f"/cloudsql/{project_id}:{instance_region}:{instance_name}"})
    table = os.environ.get("table")

    file_name_value = event["name"]
    timestamp_value = datetime.now(gettz("Europe/London")).strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"Processing file: {file_name_value}.")
    logging.info(f"Timestamp value: {timestamp_value}.")

    # Create a sql pool connection
    pool = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL(
            drivername=drivername,
            username=username,
            password=password,
            database=database,
            query=query_string,
        ),
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800
    )

    # creating a pandas DF using the values
    data = np.array([file_name_value, timestamp_value])
    df = pd.DataFrame([data], columns=["file_name","timestamp"])

    try:
        db_connection = pool.connect()
        frame = df.to_sql(table, db_connection, if_exists="append", index=False)
        db_connection.close()
        logging.info("Rows inserted into table successfully...")
    except Exception as e:
        return 'Error: {}'.format(str(e))
    return 'ok