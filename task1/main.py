import sqlalchemy
from sqlalchemy import insert
from datetime import datetime
import logging

table_name = "files_table"
connection_name = "proj_name:region:db-instance-name"
region = "region_name"


def insert_into_table(event, context):
    file_name_value = event["name"]
    timestamp_value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    stmt = (
    insert(table_name).
    values(file_name=file_name_value, time_stamp=timestamp_value)
    )
    
    db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
        drivername="mysql+pymysql",
        username="***",
        password="***",
        database="files_db",
        query=dict({"unix_socket": "/cloudsql/{}".format(connection_name)}),
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )
    try:
      conn = db.connect()
      conn.execute(stmt)
      conn.close()
      logging.info("Values inserted into table successfully...")
    except Exception as e:
        return 'Error: {}'.format(str(e))
    return 'ok'