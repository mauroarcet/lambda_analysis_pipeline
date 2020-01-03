import boto3
import psycopg2
from config import config


def main():
    rds = boto3.client('rds')
    database_type = "postgresql+psycopg2"
    username = "postgres"
    password = "s80*JzuUrsy8"
    rds_host = "database-2.canrzxdvzoof.us-east-2.rds.amazonaws.com"
    port = "5432"
    db_name = "realStateDevelopmentDb"
    connection_string = "%s://%s%s@%s:%s/%s" % (
        database_type, username, password, rds_host, port, db_name)

    conn = None
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("""SELECT * FROM coordinates""")
        row = cur
        query_results = cur.fetchall()
        print(query_results)
        cur.close()
        conn.close()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == '__main__':
    main()
