# app.py

from flask import Flask, send_file, request, jsonify
from dash import Dash
import dash_bootstrap_components as dbc
from dash import html
from dash import dcc
from dash.dependencies import Input, Output
from dash_app import make_dash, make_layout, define_callbacks, make_connection, fetch_data, accident_counts_average
from routes import blue_route, create_bikelane_network

if __name__ == '__main__':
    connection, cursor = make_connection()
    accident_df, boundary_df = fetch_data(connection)
    _, graph = create_bikelane_network()

    server = Flask(__name__)
    app = make_dash(server)
    app.layout = make_layout(accident_df,boundary_df)
    define_callbacks(app, accident_df, boundary_df)
    
    # Define the route /blue to call the blue_route function
    @server.route('/blue', methods=['POST'])
    def blue_route_map():
        # Get JSON data from the request
        data = request.get_json()

        # Extract start and destination points from the JSON data
        start_lon = float(data.get('start_lon', 144.96))
        start_lat = float(data.get('start_lat', -37.768))
        dest_lon = float(data.get('dest_lon', 144.9512))
        dest_lat = float(data.get('dest_lat', -37.796))

        # Generate the map using blue_route
        map_fig = blue_route((start_lon, start_lat), (dest_lon, dest_lat), graph)

        # Return the map as JSON response
        return jsonify(map_fig.to_dict())

    app.run_server(debug=True)