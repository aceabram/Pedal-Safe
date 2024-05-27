import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import mapping
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
from sshtunnel import SSHTunnelForwarder
import psycopg2
import os
import dash_bootstrap_components as dbc
from flask import Flask

# SSH tunnel configuration
ec2_public_dns = 'ec2-13-238-251-201.ap-southeast-2.compute.amazonaws.com'
ssh_username = 'ubuntu'
ssh_pkey = 'PedalSafe-Vijay_Keypair.pem'
rds_instance_access_point = 'database-ec2-1.cxkqw8uo29xn.ap-southeast-2.rds.amazonaws.com'
rds_port = 5432  # PostgreSQL default port

# Set Mapbox Access Token
mapbox_access_token = 'YOUR_MAPBOX_ACCESS_TOKEN'

# Define global variables
accident_counts_average = None
boundary_df = None

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


def fetch_data(connection):
    cur = connection.cursor()
    # Fetch data from tables
    cur.execute("SELECT * FROM accident;")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    accident_df = pd.DataFrame(data=rows, columns=columns)

    cur.execute("SELECT * FROM Boundary;")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    boundary_df = pd.DataFrame(data=rows, columns=columns)
    boundary_df['mccid_gis'] = boundary_df['mccid_gis'].astype(int)

    # Calculate accident counts average
    global accident_counts_average
    accident_counts_average = accident_df.groupby('severity').size().reset_index(name='accident_count')

    return accident_df, boundary_df

def create_choropleth_map(accident_df, boundary_df):
    # Create GeoDataFrame for accidents
    accidents_gdf = gpd.GeoDataFrame(
        accident_df,
        geometry=gpd.points_from_xy(accident_df.longitude, accident_df.latitude)
    )

    # boundary df to gdf
    boundary_df['polygon_geometry'] = boundary_df['polygon_geometry'].apply(wkt.loads)
    boundary_gdf = gpd.GeoDataFrame(boundary_df, geometry=boundary_df['polygon_geometry'])

    # Spatial join between accidents and boundaries to count the number of accidents per area
    boundary_gdf.crs = accidents_gdf.crs  # Ensure both GeoDataFrames have the same coordinate reference system
    accident_counts = gpd.sjoin(accidents_gdf, boundary_gdf, how='inner', predicate='within')
    #print(accident_counts.columns)
    accident_counts_per_area = accident_counts.groupby('mccid_gis_left').size().reset_index(name='accident_count')

    # Merge accident counts back to boundary GeoDataFrame
    accident_counts_per_area['mccid_gis'] = accident_counts_per_area['mccid_gis_left'].astype(int)
    boundary_gdf['mccid_gis'] = boundary_gdf['mccid_gis'].astype(int)

    boundary_gdf = boundary_gdf.merge(accident_counts_per_area, on='mccid_gis', how='left')
    boundary_gdf['accident_count'] = boundary_gdf['accident_count'].fillna(0)
    
    # Create the GeoJSON object needed for the Choroplethmapbox
    boundary_geojson = {
        'type': 'FeatureCollection',
        'features': [
            {'type': 'Feature',
            'id': row['mccid_gis'],
            'properties': {'name': row['area_name'], 'accident_count': row['accident_count']},
            'geometry': mapping(row['geometry'])} for index, row in boundary_gdf.iterrows()
        ]
    }

    # Create choropleth map
    fig = px.choropleth_mapbox(
        boundary_gdf,
        geojson=boundary_geojson,
        locations='mccid_gis',
        color='accident_count',
        color_continuous_scale='ylorbr',
        range_color=(0, boundary_gdf['accident_count'].max()),
        mapbox_style='carto-positron',
        zoom=10,
        center={
            'lat': boundary_gdf.centroid.y.mean(),
            'lon': boundary_gdf.centroid.x.mean()
        },
        opacity=0.8,
        labels={'accident_count': 'Accident Count'}
    )

    return fig

def create_bar_chart(accident_df, boundary_df, clickData, accident_counts_average):
    if clickData is not None:
        mccid_gis = clickData['points'][0]['location']
        area_name = boundary_df[boundary_df['mccid_gis'] == mccid_gis]['area_name'].iloc[0]
        filtered_df = accident_df[accident_df['mccid_gis'] == mccid_gis].copy()
        severity_counts = filtered_df.groupby('severity').size().reset_index(name='count')
        all_severity_categories = ['Fatal accident', 'Serious injury', 'Minor injury']
        severity_counts = severity_counts.set_index('severity').reindex(all_severity_categories).fillna(0).reset_index()
        severity_colors = {'Fatal accident': 'red','Serious injury': 'orange', 'Minor injury': 'yellow'}
        bar_fig = px.bar(
            severity_counts,
            x='severity',
            y='count',
            color='severity',
            title=f'Accident Severity for {area_name}',
            labels={'count': 'Count', 'severity': 'Severity'},
            text='count',
            color_discrete_map=severity_colors,
            category_orders={"severity": all_severity_categories}
        )
        bar_fig.update_layout(showlegend=True)
        bar_fig.update_traces(textposition='outside', texttemplate='%{text}', cliponaxis=False)
        return bar_fig
    else:
        severity_colors = {'Fatal accident': 'red','Serious injury': 'orange', 'Minor injury': 'yellow'}
        all_severity_categories = ['Fatal accident', 'Serious injury', 'Minor injury']
        bar_fig_base = px.bar(
            accident_counts_average,
            x='severity',
            y='accident_count',
            color='severity',
            title=f'Accident Severity for all Melbourne',
            labels={'accident_count': 'Count', 'severity': 'Severity'},
            text='accident_count',
            color_discrete_map=severity_colors,
            category_orders={"severity": all_severity_categories}
        )
        return bar_fig_base


def create_line_chart(accident_df, boundary_df, clickData):
    if clickData is not None:
        mccid_gis = clickData['points'][0]['location']
        area_name = boundary_df[boundary_df['mccid_gis'] == mccid_gis]['area_name'].values[0]
        filtered_df = accident_df[accident_df['mccid_gis'] == mccid_gis].copy()
        filtered_df['date_time'] = pd.to_datetime(filtered_df['date_time'])
        filtered_df.loc[:, 'hour'] = filtered_df['date_time'].map(lambda x: x.hour)
        accident_counts_time = filtered_df.groupby('hour').size().reset_index(name='count')
        line_fig = px.line(
            accident_counts_time,
            x='hour',
            y='count',
            title=f'Accident Count Over Time for {area_name}',
            labels={'count': 'Count', 'date_time': 'Time'},
            line_shape="spline",
            markers=True
        )
        line_fig.update_layout(xaxis={'tickmode': 'linear'})
        return line_fig
    else:
        accident_all_df = accident_df.copy()
        accident_all_df['date_time'] = pd.to_datetime(accident_all_df['date_time'])
        accident_all_df.loc[:, 'hour'] = accident_all_df['date_time'].map(lambda x: x.hour)
        accident_all_counts_time = accident_all_df.groupby('hour').size().reset_index(name='count')
        line_fig_base = px.line(
            accident_all_counts_time,
            x='hour',
            y='count',
            title=f'Accident Count Over Time for all Melbourne',
            labels={'count': 'Count', 'date_time': 'Time'},
            line_shape="spline",
            markers=True
        )
        line_fig_base.update_layout(xaxis={'tickmode': 'linear'})
        return line_fig_base


def make_dash(server):
    app = Dash(__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP], requests_pathname_prefix='/',)
    return app


def make_layout(accident_df,boundary_df):
    layout = dbc.Container([
        html.Div([
            dcc.Graph(
                figure = create_choropleth_map(accident_df, boundary_df),
                id='choropleth-map',
                className='col-md-6',
                style={'height': '80vh'}
            ),
            html.Div([
                dcc.Graph(
                    id='bar-chart',
                    className='col-md-12',
                    style={'height': '40vh'}
                ),
                dcc.Graph(
                    id='line-chart',
                    className='col-md-12',
                    style={'height': '40vh'}
                )
            ], className='col-md-6')
        ], className='row')
    ])
    return layout


def define_callbacks(app, accident_df, boundary_df):
    @app.callback(
        Output('bar-chart', 'figure'),
        [Input('choropleth-map', 'clickData')]
    )
    def update_bar_chart(clickData):
        bar_fig = create_bar_chart(accident_df, boundary_df, clickData, accident_counts_average)
        return bar_fig

    @app.callback(
        Output('line-chart', 'figure'),
        [Input('choropleth-map', 'clickData')]
    )
    def update_line_chart(clickData):
        line_fig = create_line_chart(accident_df, boundary_df, clickData)
        return line_fig