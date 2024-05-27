# data manipulation

import geopandas as gpd
import pandas as pd
import numpy as np
import networkx as nx
from shapely import wkt
from shapely.geometry import LineString, Point, MultiLineString

# database connection
from sshtunnel import SSHTunnelForwarder
import psycopg2

# plotting libraries
import geoplot.crs as gcrs
import folium

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


conn, cur = make_connection()
#  accident dataframe
cur.execute("SELECT * FROM accident;")
rows = cur.fetchall()
columns = [desc[0] for desc in cur.description]
accident_df = pd.DataFrame(data=rows, columns=columns)
accidents_gdf = gpd.GeoDataFrame(
    accident_df,
    geometry=gpd.points_from_xy(accident_df.longitude, accident_df.latitude)
)

# boundary dataframe
cur.execute("SELECT * FROM Boundary;")
rows = cur.fetchall()
columns = [desc[0] for desc in cur.description]
boundary_df = pd.DataFrame(data=rows, columns=columns)
boundary_df['mccid_gis'] = boundary_df['mccid_gis'].astype(int)

# bikelanes dataframe
cur.execute("SELECT * FROM Bikelanes;")
rows = cur.fetchall()
columns = [desc[0] for desc in cur.description]
bikelanes_df = pd.DataFrame(data=rows, columns=columns)
bikelanes_df['geometry'] = bikelanes_df['geometry'].apply(wkt.loads)
bikelanes_gdf = gpd.GeoDataFrame(bikelanes_df, geometry=bikelanes_df['geometry'])



def find_nearest_lane(accident):
    buffer_distance = 0.005
    buffer_zone = accident['geometry'].buffer(buffer_distance)
    near_bikelanes = bikelanes_gdf[bikelanes_gdf.geometry.intersects(buffer_zone)].reset_index()
    try:
        if near_bikelanes.empty:
            return None 
        else:
            nearest_bikelane = near_bikelanes.loc[[near_bikelanes.geometry.distance(accident['geometry']).idxmin()]]
            return nearest_bikelane['lane_number'].iloc[0]
    except ValueError:
        print(nearest_bikelane['lane_number'])

# Perform a spatial join to associate each accident event with the corresponding bike lane
accidents_gdf['bike_lane_id'] = accidents_gdf.apply(lambda row: find_nearest_lane(row), axis=1)
accidents_gdf.dropna(inplace=True)

# Count the number of accidents associated with each bike lane
accident_counts = accidents_gdf['bike_lane_id'].value_counts()
accident_counts_df = accident_counts.reset_index()
accident_counts_df.columns = ['bike_lane_id', 'count']

# Add a new column to the bike lanes dataset with the accident category
bikelanes_joined = bikelanes_gdf.merge(accident_counts_df, left_on='lane_number', right_on='bike_lane_id', how='left')
bikelanes_joined['count'] = bikelanes_joined['count'].fillna(0)
bikelanes_joined.drop(columns=['bike_lane_id'], inplace=True)

from math import radians, sin, cos, sqrt, atan2

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth radius in kilometers

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Calculate the differences
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Calculate the distance using the Haversine formula
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c

    return distance

def get_color(accident_count):
    if accident_count == 0:
        return 'green'
    elif 1 <= accident_count < 5:
        return 'yellow'
    elif 5 <= accident_count < 10:
        return 'orange'
    else:
        return 'red'

def get_square_bounds(point_a, point_b):
    lat_a, lon_a = point_a
    lat_b, lon_b = point_b
    xmin = min(lon_a, lon_b)
    ymin = min(lat_a, lat_b)
    xmax = max(lon_a, lon_b)
    ymax = max(lat_a, lat_b)
    return [(ymin, xmin), (ymax, xmax)]
   
# Extract the cycle lanes and create network graph 
def create_bikelane_network():
    cycle_lanes = []

    # for geom in bikelanes_joined.geometry:
    #     if isinstance(geom, LineString):
    #         cycle_lanes.append(geom.coords)
    #     elif isinstance(geom, MultiLineString):
    #         for line in geom.geoms:
    #             cycle_lanes.append(line.coords)
    
    # # Create a graph representation of the cycle lanes
    # G = nx.Graph()
    # for line_coords in cycle_lanes:
    #     for i in range(len(line_coords) - 1):
    #         G.add_edge(line_coords[i], line_coords[i + 1])            
    
    G = nx.Graph()
    for idx, row in bikelanes_joined.iterrows():
        geom = row['geometry']
        accident_count = row['count']
        #accident_count = accident_counts.get(idx, 0)  # Assuming 'accident_counts' maps IDs to counts
        if isinstance(geom, LineString):
            coords = list(geom.coords)
            cycle_lanes.append(geom.coords)
            for i in range(len(coords) - 1):
                G.add_edge(coords[i], coords[i + 1], weight=accident_count)
        elif isinstance(geom, MultiLineString):
            for line in geom.geoms:
                coords = list(line.coords)
                cycle_lanes.append(line.coords)
                for i in range(len(coords) - 1):
                    G.add_edge(coords[i], coords[i + 1], weight=accident_count)


    return cycle_lanes, G                 


def blue_route(start_point, end_point, bikelane_network):
    G = bikelane_network
    # Create a map
    m = folium.Map(location=[-37.790231531464,144.951780451295], zoom_start=14,tiles="cartodb positron")  
    
    MAX_DISTANCE_THRESHOLD = 1.5

    # Find the nearest nodes in the graph to the start and end points
    start_node_index = np.argmin([Point(node).distance(Point(start_point)) for node in G.nodes])
    end_node_index = np.argmin([Point(node).distance(Point(end_point)) for node in G.nodes])
    start_node = list(G.nodes)[start_node_index]
    end_node = list(G.nodes)[end_node_index]

    

    # Check if the nearest nodes are within the range of the bike lanes
    start_distance = round(haversine(start_point[1], start_point[0], start_node[1], start_node[0]),2)
    end_distance = round(haversine(end_point[1], end_point[0], end_node[1], end_node[0]),2)

    start_far = start_distance > MAX_DISTANCE_THRESHOLD
    end_far = end_distance > MAX_DISTANCE_THRESHOLD
    
    if start_far or end_far:
        message = "Start" if start_far else "End"
        print(f"{message} point is out of range of bike lanes ({start_distance if start_far else end_distance} km away).")
    else:
        try:
            # Find the shortest path between the start and end nodes
            path_nodes = nx.shortest_path(G, source=start_node, target=end_node)

            # Convert the path nodes to coordinates
            path_coords = [[node[1], node[0]] for node in path_nodes]

            # Add the route to the map
            folium.PolyLine(locations=path_coords, color='#1f77b4', weight=10).add_to(m)
        except nx.NetworkXNoPath:
            print("No path found between the start and end points.")
            bounds = get_square_bounds(start_point, end_point)
            bounds = [(j,i)for i,j in bounds]
            

            center_lat = (bounds[0][0] + bounds[1][0]) / 2
            center_lon = (bounds[0][1] + bounds[1][1]) / 2
            m.location = [center_lat, center_lon]
            m.zoom_start = 20

            # Display bike routes within the square area
            # Display bike routes within the square area
            for edge in G.edges():
                u, v = edge
                u_lon, u_lat = u
                v_lon, v_lat = v
                if (u_lat >= bounds[0][0] and u_lat <= bounds[1][0] and
                    u_lon >= bounds[0][1] and u_lon <= bounds[1][1] and
                    v_lat >= bounds[0][0] and v_lat <= bounds[1][0] and
                    v_lon >= bounds[0][1] and v_lon <= bounds[1][1]):
                    color = get_color(G[u][v]['weight'])
                    folium.PolyLine(locations=[(u[1], u[0]), (v[1], v[0])], color='#1f77b4', weight=10).add_to(m)

    nearest_node = start_node
    
    if start_distance <= MAX_DISTANCE_THRESHOLD:
        
        folium.PolyLine(locations=[(start_point[1], start_point[0]), (nearest_node[1], nearest_node[0])], color='black', weight=2, dash_array='5,5').add_to(m)
    
    if end_distance <= MAX_DISTANCE_THRESHOLD:
        
        folium.PolyLine(locations=[(end_point[1], end_point[0]), (end_node[1], end_node[0])], color='black', weight=5, dash_array='5,5').add_to(m)


    folium.CircleMarker(location=(start_point[1],start_point[0]), radius=2,color='green', weight=5).add_to(m)
    folium.CircleMarker(location=(end_point[1],end_point[0]),radius=2,color='red', weight=5).add_to(m)        
            
    # display map
    return m    
        

def colored_route(start_point, end_point, bikelane_network):
    G = bikelane_network
    # Create a map
    m = folium.Map(location=[-37.790231531464,144.951780451295], zoom_start=14,tiles="cartodb positron")  
    
    MAX_DISTANCE_THRESHOLD = 1.5

    # Find the nearest nodes in the graph to the start and end points
    start_node_index = np.argmin([Point(node).distance(Point(start_point)) for node in G.nodes])
    end_node_index = np.argmin([Point(node).distance(Point(end_point)) for node in G.nodes])
    start_node = list(G.nodes)[start_node_index]
    end_node = list(G.nodes)[end_node_index]
    

   

    # Check if the nearest nodes are within the range of the bike lanes
    start_distance = round(haversine(start_point[1], start_point[0], start_node[1], start_node[0]),2)
    end_distance = round(haversine(end_point[1], end_point[0], end_node[1], end_node[0]),2)
    
    start_far = start_distance > MAX_DISTANCE_THRESHOLD
    end_far = end_distance > MAX_DISTANCE_THRESHOLD
    
    if start_far or end_far:
        message = "Start" if start_far else "End"
        print(f"{message} point is out of range of bike lanes ({start_distance if start_far else end_distance} km away).")

    else:
        try:
            #Find the shortest path between the start and end nodes
            path_nodes = nx.shortest_path(G, source=start_node, target=end_node)

            for node in path_nodes[:-1]:
                next_node = path_nodes[path_nodes.index(node) + 1]
                color = get_color(G[node][next_node]['weight'])
                # Add the route to the map
                folium.PolyLine(locations=[(node[1], node[0]), (next_node[1], next_node[0])], color=color, weight=10).add_to(m)
            
            
        except nx.NetworkXNoPath:
            print("No path found between the start and end points.")
            # Display a square area around the start and end points
            bounds = get_square_bounds(start_point, end_point)
            bounds = [(j,i)for i,j in bounds]
            

            center_lat = (bounds[0][0] + bounds[1][0]) / 2
            center_lon = (bounds[0][1] + bounds[1][1]) / 2
            m.location = [center_lat, center_lon]
            m.zoom_start = 20

            # Display bike routes within the square area
            # Display bike routes within the square area
            for edge in G.edges():
                v, u = edge
                u_lon, u_lat = u
                v_lon, v_lat = v
                if (u_lat >= bounds[0][0] and u_lat <= bounds[1][0] and
                    u_lon >= bounds[0][1] and u_lon <= bounds[1][1] and
                    v_lat >= bounds[0][0] and v_lat <= bounds[1][0] and
                    v_lon >= bounds[0][1] and v_lon <= bounds[1][1]):
                    color = get_color(G[u][v]['weight'])
                    folium.PolyLine(locations=[(u[1], u[0]), (v[1], v[0])], color=color, weight=5).add_to(m)

                
    # Add a dotted line between start_point and nearest bike lane node
    nearest_node = start_node
    if start_distance <= MAX_DISTANCE_THRESHOLD:
        
        folium.PolyLine(locations=[(start_point[1], start_point[0]), (nearest_node[1], nearest_node[0])], color='black', weight=5, dash_array='5,5').add_to(m)
    
    if end_distance <= MAX_DISTANCE_THRESHOLD:
        
        folium.PolyLine(locations=[(end_point[1], end_point[0]), (end_node[1], end_node[0])], color='black', weight=5, dash_array='5,5').add_to(m)

    folium.CircleMarker(location=(start_point[1], start_point[0]), radius=2, color='green', weight=5).add_to(m)
    folium.CircleMarker(location=(end_point[1], end_point[0]), radius=2, color='red', weight=5).add_to(m)


    # display map
    return m
        


_,graph = create_bikelane_network()
start_tuple = (144.96,-37.768)
destination_tuple = (144.9512,-37.796) 

# start_tuple = (144.9512, -37.796)
# destination_tuple = (144.98818764532,-37.814201226257) 

fig = blue_route(start_tuple, destination_tuple, graph)
fig        