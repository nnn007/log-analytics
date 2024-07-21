import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, dash_table
import logging
import os
import time
import tempfile
import base64
import plotly.express as px
from log_parser import parse_logs
from log_analyzer import analyze_logs

logger = logging.getLogger(__name__)

app_layout = html.Div([
    html.H1("Log Analytics Dashboard"),
    dcc.Upload(
        id='upload-data',
        children=html.Div(['Drag and Drop or ', html.A('Select Files')]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        multiple=True
    ),
    html.Div(id='file-list'),
    html.Button('Analyze Logs', id='analyze-button', n_clicks=0),
    dcc.Loading(
        id="loading",
        children=[html.Div(id='output-data-upload')],
        type="circle",
    )
])


def update_file_list(uploaded_filenames):
    if not uploaded_filenames:
        return 'No files uploaded yet.'
    return html.Ul([html.Li(filename) for filename in uploaded_filenames])


def update_output(n_clicks, list_of_contents, list_of_names):
    if n_clicks == 0 or list_of_contents is None:
        return html.Div('No files analyzed yet.')

    temp_dir = tempfile.mkdtemp()
    temp_files = []

    try:
        start_time = time.time()
        for content, name in zip(list_of_contents, list_of_names):
            content_type, content_string = content.split(',')
            decoded = base64.b64decode(content_string)
            temp_file_path = os.path.join(temp_dir, name)
            with open(temp_file_path, 'wb') as f:
                f.write(decoded)
            temp_files.append(temp_file_path)

        log_df = parse_logs(temp_files)
        results = analyze_logs(log_df)
        end_time = time.time()
        processing_time = end_time - start_time

        return html.Div([
            html.H5(f'Results for {len(list_of_names)} files:'),
            html.Div(f'Processing Time: {processing_time:.2f} seconds'),

            html.H6("Top Log Templates:"),
            dcc.Graph(figure=px.bar(results['template_counts'].head(20), x='count', y='log_template', orientation='h',
                                    title="Top Log Templates")),

            html.H6("Top Error Templates:"),
            dash_table.DataTable(
                data=results['error_templates'].head(20).to_dict('records'),
                columns=[{"name": i, "id": i} for i in results['error_templates'].columns],
                style_table={'height': '300px', 'overflowY': 'auto'}
            ),

            html.H6("Top Clusters:"),
            dash_table.DataTable(
                data=results['cluster_counts'].head(20).to_dict('records'),
                columns=[{"name": i, "id": i} for i in results['cluster_counts'].columns],
                style_table={'height': '300px', 'overflowY': 'auto'}
            ),

            html.H6("Top Words:"),
            dcc.Graph(figure=px.bar(results['word_counts'].head(50), x='count', y='word', orientation='h',
                                    title="Top Words")),

            html.H6("Log Topics:"),
            html.Ul([html.Li(", ".join(topic)) for topic in results['topics']]),

            html.H6("Co-occurring Log Templates:"),
            dash_table.DataTable(
                data=results['template_pairs'].to_dict('records'),
                columns=[{"name": i, "id": i} for i in results['template_pairs'].columns],
                style_table={'height': '300px', 'overflowY': 'auto'}
            ),

            html.H6("Log Statistics:"),
            html.Div([
                html.P(f"Total Log Entries: {len(log_df)}"),
                html.P(f"Unique Log Templates: {len(results['template_counts'])}"),
                html.P(f"Average Log Length: {results['avg_log_length']:.2f} characters")
            ])
        ])
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}", exc_info=True)
        return html.Div(f'Error processing files: {str(e)}')
    finally:
        for file in temp_files:
            os.remove(file)
        os.rmdir(temp_dir)
