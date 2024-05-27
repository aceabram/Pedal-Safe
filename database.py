# Import libraries
from sshtunnel import SSHTunnelForwarder
import psycopg2
from psycopg2 import extras
from psycopg2 import sql
import pandas as pd
import os

# SSH tunnel configuration
ec2_public_dns = 'ec2-13-238-251-201.ap-southeast-2.compute.amazonaws.com'
ssh_username = 'ubuntu'
ssh_pkey = 'PedalSafe-Vijay_Keypair.pem'
rds_instance_access_point = 'database-ec2-1.cxkqw8uo29xn.ap-southeast-2.rds.amazonaws.com'
rds_port = 5432  # PostgreSQL default port

# Establish SSH tunnel
def make_connection():
    try:
        tunnel = SSHTunnelForwarder(ec2_public_dns,
                                    ssh_username=ssh_username,
                                    ssh_pkey=ssh_pkey,
                                    remote_bind_address=(rds_instance_access_point, rds_port))
        tunnel.start()
        print("****SSH Tunnel Established****")

        connection = psycopg2.connect(
            dbname="PedalSafe_EC2_Master_DB",
            user="postgres",
            password="miBjRVM9LG3GdYS",
            host='127.0.0.1',  # Connect to localhost since we're tunneling
            port=tunnel.local_bind_port
        )
        print('Database connected')
        cursor = connection.cursor()
        return connection, cursor
    except psycopg2.Error as e:
        print("Error establishing connection:", e)
        raise

def exec_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        cursor.close()
        connection.commit()
    except psycopg2.Error as e:
        print("Error executing query:", e)
        raise


# -------------------1. BOUNDARY/POSTCODES DATA--------------------------------

# import data from csv
table_name = 'Boundary'
boundary_df = pd.read_csv(f'{os.getcwd()}/../data/boundary.csv')
# drop any existing table
drop_table_query = sql.SQL(f"DROP TABLE IF EXISTS {table_name};")

conn, curs = make_connection()
exec_query(conn, drop_table_query)
# create new table from defined schema
create_table_query = sql.SQL(f"""CREATE TABLE {table_name}(
    mccid_gis VARCHAR(255),
    geo_point_2d VARCHAR(255),
    polygon_geometry TEXT,
    area_name VARCHAR(100),
    PRIMARY KEY (mccid_gis)
    ); """)

conn, curs = make_connection()
exec_query(conn,create_table_query)

# insert values into table
conn, cur = make_connection()
# cur = conn.cursor()


tuples = [tuple(x) for x in boundary_df.to_numpy()]
insert_query = f"INSERT INTO {table_name} VALUES %s"
extras.execute_values(cur, insert_query, tuples)
conn.commit()
print(f'Data inserted in {table_name}')

# -------------------2. BIKELANES DATA--------------------------------
# import data from csv
table_name = 'bikelanes'
bikelanes_df = pd.read_csv(f'{os.getcwd()}/../data/bikelanes.csv')

# drop any existing table
drop_table_query = sql.SQL(f"DROP TABLE IF EXISTS {table_name};")
conn, curs = make_connection()
exec_query(conn, drop_table_query)

# create new table from defined schema
create_table_query = sql.SQL(f"""CREATE TABLE {table_name}(
    lane_number INT PRIMARY KEY,
    geo_point_2d VARCHAR(255) NOT NULL, 
    type VARCHAR(255) NOT NULL,
    geometry TEXT  NOT NULL,
    count FLOAT 
    ); """)

conn, curs = make_connection()
exec_query(conn, create_table_query)

# insert values into table
conn, cur = make_connection()

tuples = [tuple(x) for x in bikelanes_df.to_numpy()]
insert_query = f"INSERT INTO {table_name} VALUES %s"
extras.execute_values(cur, insert_query, tuples)
conn.commit()
print(f'Data inserted in {table_name}')

# -------------------3. CRIME DATA--------------------------------
# import data from csv
table_name = 'crimes'
crimes_df = pd.read_csv(f'{os.getcwd()}/../data/crimes.csv')

# drop any existing table
drop_table_query = sql.SQL(f"DROP TABLE IF EXISTS {table_name};")
conn, curs = make_connection()
exec_query(conn, drop_table_query)


create_table_query = sql.SQL(f"""CREATE TABLE {table_name} (                    
    record_number INT PRIMARY KEY,
    year INT,                    
    lga VARCHAR(255),
    suburb VARCHAR(255),
    offence_division VARCHAR(255),
    offence_subdivision VARCHAR(255),
    offence_subgroup VARCHAR(255),
    count INT );""")

conn, curs = make_connection()
exec_query(conn, create_table_query)

# insert values into table
conn, cur = make_connection()

tuples = [tuple(x) for x in crimes_df.to_numpy()]
insert_query = f"INSERT INTO {table_name} VALUES %s"
extras.execute_values(cur, insert_query, tuples)
conn.commit()
print(f'Data inserted in {table_name}')

# -------------------4. ACCIDENT DATA--------------------------------

# import data from csv
table_name = 'accident'
accident_df = pd.read_csv(f'{os.getcwd()}/../data/accident.csv')

# drop any existing table
drop_table_query = sql.SQL(f"DROP TABLE IF EXISTS {table_name};")

conn, curs = make_connection()
exec_query(conn, drop_table_query)

# create new table from defined schema
create_table_query = sql.SQL(f"""CREATE TABLE {table_name}(
    accident_no VARCHAR(255) PRIMARY KEY,
    accident_type VARCHAR(255),
    day_of_week VARCHAR(255),
    light_condition VARCHAR(255),
    road_geometry VARCHAR(255),
    severity VARCHAR(255),
    speed_zone VARCHAR(255),
    node_type VARCHAR(255),
    lga_name VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    fatality INTEGER,
    serious_injury INTEGER,
    other_injury INTEGER,
    non_injured INTEGER,
    males INTEGER,
    females INTEGER,
    bicyclist INTEGER,
    passenger INTEGER,
    driver INTEGER,
    pedestrian INTEGER,
    pillion INTEGER,
    motorcyclist INTEGER,
    unknown INTEGER,
    ped_cyclist_5_12 INTEGER,
    ped_cyclist_13_18 INTEGER,
    old_ped_65_and_over INTEGER,
    old_driver_75_and_over INTEGER,
    young_driver_18_25 INTEGER,
    no_of_vehicles INTEGER,
    heavy_vehicle INTEGER,
    passenger_vehicle INTEGER,
    motorcycle INTEGER,
    pt_vehicle INTEGER,
    rma VARCHAR(255),
    divided VARCHAR(255),
    geometry TEXT,
    mccid_gis INTEGER,
    suburb VARCHAR(255),
    date_time TIMESTAMP,
    bike_lane_id FLOAT                     
    ); """)

conn, curs = make_connection()
exec_query(conn, create_table_query)

# insert values into table
conn, cur = make_connection()

tuples = [tuple(x) for x in accident_df.to_numpy()]
insert_query = f"INSERT INTO {table_name} VALUES %s"
extras.execute_values(cur, insert_query, tuples)
conn.commit()
print(f'Data inserted in {table_name}')

# -------------------5. BIKERAILS DATA--------------------------------
# import data from csv
table_name = 'bikerails'
bikerails_df = pd.read_csv(f'{os.getcwd()}/../data/bikerails.csv')

# drop any existing table
drop_table_query = sql.SQL(f"DROP TABLE IF EXISTS {table_name};")

conn, curs = make_connection()
exec_query(conn, drop_table_query)

create_table_query = sql.SQL(f"""CREATE TABLE {table_name} (                    
    gis_id INT PRIMARY KEY,
    description VARCHAR(255),
    asset_class VARCHAR(50),
    asset_type VARCHAR(50),
    model_descr VARCHAR(255),
    company VARCHAR(100),
    geometry TEXT );""")

conn, curs = make_connection()
exec_query(conn, create_table_query)

# insert values into table
conn, cur = make_connection()

tuples = [tuple(x) for x in bikerails_df.to_numpy()]
insert_query = f"INSERT INTO {table_name} VALUES %s"
extras.execute_values(cur, insert_query, tuples)
conn.commit()
print(f'Data inserted in {table_name}')




# -------------------2. ALL ROADS DATA--------------------------------
# import data from csv
table_name = 'Allroads'
all_roads_df = pd.read_csv(f'{os.getcwd()}/../data/all_roads.csv')
print('data loaded')
# drop any existing table
drop_table_query = sql.SQL(f"DROP TABLE IF EXISTS {table_name};")
# conn, curs = make_connection()
conn, curs = make_connection()
exec_query(conn, drop_table_query)
print('table dropped')
# create new table from defined schema

create_table_query = sql.SQL(f"""CREATE TABLE {table_name}(
    lane_number INT PRIMARY KEY,
    geo_point_2d VARCHAR(255) NOT NULL, 
    geometry TEXT  NOT NULL,
    count FLOAT                        
    ); """)

conn, curs = make_connection()
exec_query(conn, create_table_query)

# insert values into table
conn, cur = make_connection()

tuples = [tuple(x) for x in all_roads_df.to_numpy()]
insert_query = f"INSERT INTO {table_name} VALUES %s"
extras.execute_values(cur, insert_query, tuples)
conn.commit()
print(f'Data inserted in {table_name}')    

select_query = sql.SQL(f"SELECT * from {table_name} LIMIT 3;")
conn, curs = make_connection()
curs.execute(select_query)
rows = curs.fetchall()
for row in rows:
    print(row)