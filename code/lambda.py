import boto3
import psycopg2

import sqlalchemy as db
from config import config


def lambda_handler(event, context):
    update_approval_states()


def connect():
    params = config()
    connect = psycopg2.connect(**params)
    return connect


def get_real_states_coordinates():
    coordinates_data = []

    try:
        print('Connecting to the AWS RDS database...')

        engine = db.create_engine('postgresql://', creator=connect)
        connection = engine.connect()
        metadata = db.MetaData()

        coordinates_table = db.Table('coordinates', metadata, autoload=True,
                                     autoload_with=engine)

        query = db.select([coordinates_table.columns.latitude,
                           coordinates_table.columns.longitude,
                           coordinates_table.columns.real_state_id])

        print("Fetching data from database...")

        result_proxy = connection.execute(query).fetchall()

        for row_proxy in result_proxy:
            coordinates_data.append(row_proxy)

        return(coordinates_data)

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)


def analyze_real_state_coordinates():
    coordinates_data = get_real_states_coordinates()
    valid_coordinates = []
    invalid_coordinates = []

    for row in coordinates_data:
        latitude = row[0]
        longitude = row[1]
        real_state_id = row[2]

        if latitude > 0 and longitude < 0:
            valid_coordinates.append(real_state_id)
        else:
            invalid_coordinates.append(real_state_id)

    return valid_coordinates, invalid_coordinates


def update_approval_states():
    valid_coordinates, invalid_coordinates = analyze_real_state_coordinates()

    try:
        engine = db.create_engine('postgresql://', creator=connect)
        connection = engine.connect()
        metadata = db.MetaData()

        real_states = db.Table(
            'real_states',
            metadata, autoload=True,
            autoload_with=engine)

        print("Updating approval state from approved real states...")
        for real_state_id in valid_coordinates:
            db.update(real_states).values(approval_state=True).where(
                real_states.columns.id == real_state_id)

        print("Updating approval state from unapproved real states...")
        for real_state_id in invalid_coordinates:
            db.update(real_states).values(approval_state=False).where(
                real_states.columns.id == real_state_id)

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

    print("Database Updated")
