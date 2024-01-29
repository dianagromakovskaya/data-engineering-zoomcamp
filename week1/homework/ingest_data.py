#!/usr/bin/env python
# coding: utf-8
import argparse
import pandas as pd
from sqlalchemy import create_engine
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    trips_table_name = params.trips_table_name
    tz_table_name = params.tz_table_name
    trips_url = params.trips_url
    tz_url = params.tz_url
    trips_csv_name = 'trips_output.csv.gz'
    tz_csv_name = 'tz_output.csv'

    os.system(f"wget {trips_url} -O {trips_csv_name}")
    os.system(f"wget {tz_url} -O {tz_csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # insert time zones data
    df = pd.read_csv(tz_csv_name)
    df.to_sql(name=tz_table_name, con=engine, if_exists='append')

    #insert trips data
    df_iter = pd.read_csv(trips_csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.head(n=0).to_sql(name=trips_table_name, con=engine, if_exists='replace')
    df.to_sql(name=trips_table_name, con=engine, if_exists='append')

    while True:
        df = next(df_iter)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.to_sql(name=trips_table_name, con=engine, if_exists='append')
        print('inserted chunk')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='pass for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='post for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--trips_table_name', help='name of the table where we will write the trips data to')
    parser.add_argument('--tz_table_name', help='name of the table where we will write the time zones data to')
    parser.add_argument('--trips_url', help='url of the trips csv file')
    parser.add_argument('--tz_url', help='url of the time zones csv file')

    args = parser.parse_args()
    print(args)
    main(args)
