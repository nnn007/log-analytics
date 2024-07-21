import os
import base64
import tempfile
import logging
import time
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, dash_table, Input, Output, State
import plotly.express as px
from flask import g

from log_parser import parse_logs
from log_analyzer import analyze_logs
from layout import app_layout, update_file_list, update_output

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Setup the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Log Analytics Dashboard"
server = app.server  # We need to access the underlying Flask server


# Register a function to close the Spark session when the app context tears down
@server.teardown_appcontext
def shutdown_spark(exception=None):
    spark = g.pop('spark', None)
    if spark:
        spark.stop()
        logger.info("Spark session stopped")


# Update the app layout
app.layout = app_layout


@app.callback(
    Output('file-list', 'children'),
    Input('upload-data', 'filename')
)
def update_file_list_callback(uploaded_filenames):
    return update_file_list(uploaded_filenames)


@app.callback(
    Output('output-data-upload', 'children'),
    Input('analyze-button', 'n_clicks'),
    State('upload-data', 'contents'),
    State('upload-data', 'filename')
)
def update_output_callback(n_clicks, list_of_contents, list_of_names):
    return update_output(n_clicks, list_of_contents, list_of_names)


if __name__ == '__main__':
    app.run_server(debug=True, port=9050)
