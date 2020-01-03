import boto3
import psycopg2
from config import config


def main():
    conn = None

    validate_coordinates_query = """SELECT real_state_id 
                    FROM coordinates
                    WHERE longitude < 0 AND latitude > 0"""

    valid_coordinates = []
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(validate_coordinates_query)

        rows = cur.fetchall()

        for row in rows:
            valid_coordinates.append(row[0])

        return(valid_coordinates)

        cur.close()

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def update_real_states_by_coordinates():
    conn = None
    update_real_state_by_coordinates_query = """UPDATE real_states
                                                SET approval_state = %s
                                                WHERE id=%s"""
    valid_coordinates = main()

    try:
        params = config()
        print('Updating approval state values from real state table...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        for real_state_id in valid_coordinates:
            cur.execute(update_real_state_by_coordinates_query, (True, real_state_id))

        updated_rows = cur.rowcount
        conn.commit()
        cur.close()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    print("Rows Updated")

if __name__ == '__main__':
    update_real_states_by_coordinates()
