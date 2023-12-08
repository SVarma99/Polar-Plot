import dash
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
import snowflake.snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.session import Session
import plotly.graph_objs as go
import time

#----------------------------------------------------------------------------
connection_parameters = {
    "account": "oneweb_operations_uk.eu-west-2.aws",
    "user": "service_powerbi_ssdh",
    "password":"pY5O2XsnNj0lil87VKPxrEffqgAIbrMzif7",#service account is good practice
    #"authenticator": "externalbrowser",
    "warehouse": "SSDH_USER_WH",
    "database": "DATAOPS_GN_FB_SV_POLAR_TEST",
    "schema": "GN_CALCULATION",
    "role": 'TENANT_DEV'
}

session = Session.builder.configs(connection_parameters).create()
#----------------------------------------------------------------------------
 
def get_unique_values(column_name):
    query = session.table("POLARTEST").select(column_name).distinct()
    return [row[0] for row in query.collect()]
 
#----------------------------------------------------------------------------
#making it look nice, mostly default HTML skins (what is this, fortnite?)
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Polar Plot"
 
app.layout = html.Div([
    html.H1("Contact Troubleshooting", style={'textAlign': 'center', 'color': '#FFFFFF', 'padding': '20px'}),
    dcc.DatePickerRange(
        id='date-picker-range',
        min_date_allowed=pd.to_datetime("2023-01-01"),
        max_date_allowed=pd.to_datetime("2023-12-31"),
        start_date=pd.to_datetime("2023-10-09"),
        end_date=pd.to_datetime("2023-10-09"),
        style={
        'backgroundColor': '#07594A', #same as SCA
        'color': 'white',  # textcolor
        'fontWeight': 'bold'} #example dates, find a way to make it empty to begin with
    ),
    dcc.Dropdown(
        id='satellite-dropdown',
        options=[{'label': i, 'value': i} for i in get_unique_values('SATELLITE_ID')],
        placeholder="Select Satellite IDs",
        multi=True,
        style={'width': '43%'}
    ),
    dcc.Dropdown(
        id='categorization-dropdown',
        options=[{'label': i, 'value': i} for i in get_unique_values('CATEGORIZATION_N')],
        placeholder="Select Categorizations",
        multi=True,
        style={'width': '43%'}
    ),
    dcc.Dropdown(
        id='sapid-dropdown',
        options=[{'label': i, 'value': i} for i in get_unique_values('SAPID')],
        placeholder="Select SAPIDs",
        multi=True,
        style={'width': '43%'}
    ),
    dcc.Dropdown(
        id='gnid-dropdown',
        options=[{'label': i, 'value': i} for i in get_unique_values('GNID')],
        placeholder="Select GNIDs",
        multi=True,
        style={'width': '43%'}
    ),
    html.Div([
        html.Div([
            dcc.Input(id='azimuth-min', type='number', placeholder= 0),
            dcc.Input(id='azimuth-max', type='number', placeholder='Max Azimuth')
        ], style={'width': '35%'}),
        html.Div([
            dcc.Input(id='elevation-min', type='number', placeholder= 0),
            dcc.Input(id='elevation-max', type='number', placeholder='Max Elevation')
        ], style={'width': '35%'})
    ]),
 
    html.Button('Plot!', id='submit-button', n_clicks=0),
    html.Button('Reset Filters', id='reset-button', n_clicks=0),
 
 
    #putting them side by side
    dbc.Row([
    dbc.Col(dcc.Graph(id='polar-plot'), width = 5),
    dbc.Col(dcc.Graph(id = 'rssi-time-plot'), width = 5)
    ]),
 
    html.Div(id='clicked-data-display'),
    dash_table.DataTable(
        id='data-table',
        columns=[],
        data=[],
        row_selectable='multi',  # Enable single row selection
        style_table={'height': '300px', 'overflowY': 'auto', 'backgroundColor': '#3a3a3a',
                     'fontWeight': 'bold'},
        style_header={
        'backgroundColor': '#7E5C0A', #same as SCA
        'color': 'white',  # textcolor
        'fontWeight': 'bold'},
        style_cell={
        'backgroundColor': '#3a3a3a',
        'color': 'white',  #cell text color
        'border': '1px solid #444'  # cell border color
    }
    ),
    html.Div(id='stored-data', style={'display': 'none'})
], style={'backgroundColor': '#3a3a3a'})
 
#----------------------------------------------------------------------------
#RESET BUTTON CALL BACKS
#similar gist as plot button, but this time we're defining
# a function that removes all the filters
 
@app.callback(
    [Output('date-picker-range', 'start_date'),
     Output('date-picker-range', 'end_date'),
     Output('satellite-dropdown', 'value'),
     Output('categorization-dropdown', 'value'),
     Output('sapid-dropdown', 'value'),
     Output('gnid-dropdown', 'value'),
     Output('azimuth-min', 'value'),
     Output('azimuth-max', 'value'),
     Output('elevation-min', 'value'),
     Output('elevation-max', 'value')],
    [Input('reset-button', 'n_clicks')]
)
def reset_filters(n_clicks):
    if n_clicks > 0:
        return [pd.to_datetime("2023-10-09"), pd.to_datetime("2023-10-09"), [], [], [], [], None, None, None, None]
    else:
        return [dash.no_update] * 10 #10 is for all 10 filters
 
#----------------------------------------------------------------------------
#callbacks!
# - inputs are what you're inputting into the state, output "stored data"
#   is how we're storing it as a json to then display in the botton as the
#   table
 
@app.callback(
    Output('stored-data', 'children'),
    [Input('submit-button', 'n_clicks')],
    [State('date-picker-range', 'start_date'),
     State('date-picker-range', 'end_date'),
     State('satellite-dropdown', 'value'),
     State('categorization-dropdown', 'value'),
     State('sapid-dropdown', 'value'),
     State('gnid-dropdown', 'value'),
     State('azimuth-min', 'value'),
     State('azimuth-max', 'value'),
     State('elevation-min', 'value'),
     State('elevation-max', 'value')]
)
#----------------------------------------------------------------------------
#----------------------------------------------------------------------------
#----------------------------------------------------------------------------
#filtering, the more the better as polar plot will display quicker
def load_data(n_clicks, start_date, end_date, selected_satellites, selected_categorizations, selected_sapids, selected_gnids, azimuth_min, azimuth_max, elevation_min, elevation_max):
    print('pre-load')

    if n_clicks < 1:
        return dash.no_update
    st = time.time()

    filtered_df = session.table("POLARTEST").filter(
        (col("DATE") >= start_date) &
        (col("DATE") <= end_date) &
        (col("SATELLITE_ID").isin(selected_satellites) if selected_satellites else True) &
        (col("CATEGORIZATION_N").isin(selected_categorizations) if selected_categorizations else True) &
        (col("SAPID").isin(selected_sapids) if selected_sapids else True) &
        (col("GNID").isin(selected_gnids) if selected_gnids else True) &
        (col("AZPOSSRSF").between(azimuth_min, azimuth_max) if azimuth_min is not None and azimuth_max is not None else True) &
        (col("ELPOSSRSF").between(elevation_min, elevation_max) if elevation_min is not None and elevation_max is not None else True)
    ).toPandas()
    print(filtered_df)
    filtered_df['RSSI_VARIANCE'] = filtered_df.groupby('CONTACT_ID_SATELLITE_GATEWAYSEC')['RSSI'].transform('var')
    filtered_df['RSSI_MIN'] = filtered_df.groupby('CONTACT_ID_SATELLITE_GATEWAYSEC')['RSSI'].transform('min')
    filtered_df['RSSI_MAX'] = filtered_df.groupby('CONTACT_ID_SATELLITE_GATEWAYSEC')['RSSI'].transform('max')
    #print(filtered_df)
    end = time.time()
    elapsed_time = end - st
    print(elapsed_time)
    print('filtered df (post load)')
    return filtered_df.to_json(date_format='iso', orient='split')
 
#----------------------------------------------------------------------------
#another callback - from chatgpt :)
@app.callback(
    Output('polar-plot', 'figure'),
    [Input('stored-data', 'children'),
    Input('data-table', 'selected_rows')],
    [State('data-table', 'data')])
#https://www.youtube.com/watch?v=pNMWbY0AUJ0 - callback vid is useful
 
 
def update_polar_plot(jsonified_cleaned_data, selected_rows, rows_data):
    if not jsonified_cleaned_data:
        return dash.no_update
 
    df = pd.read_json(jsonified_cleaned_data, orient='split')
 
    if selected_rows:
        filtered_dfs = [df[df['CONTACT_ID_SATELLITE_GATEWAYSEC'] == rows_data[row]['CONTACT_ID_SATELLITE_GATEWAYSEC']] for row in selected_rows]
        if filtered_dfs:
            df = pd.concat(filtered_dfs, ignore_index=True)
    else:
        # If no rows are selected, show the full dataset or handle appropriately
        pass
    #df = pd.read_json(jsonified_cleaned_data, orient='split')
 
    custom_color_scale = [
    (0, 'red'),        # Red for the lowest values
    (10/25, 'red'),    # Red at RSSI = 10
    (10/25, '#00FF7F'),  # Green just above RSSI = 10
    (1, '#00FF7F')       # Green for the highest values
]
    print('preplot')
    st_p = time.time()

    fig = px.scatter_polar(
        df,
        r='ELPOSSRSF',
        theta='AZPOSSRSF',
        color='RSSI',
        hover_data=["CONTACT_ID_SATELLITE_GATEWAYSEC", "GNID", "TIMESTAMP", "SATELLITE_ID", "SAPID", "SRSF_START_TIME", "SRSF_END_TIME", "CATEGORIZATION_N"],
        color_continuous_scale=custom_color_scale,
        range_color=[0, 25])
    
    end_p = time.time()
    elapsed_time_p = end_p - st_p
    print(elapsed_time_p)
    print('post-plot')
    print(df)
    fig.update_traces(marker=dict(size=3), hoverlabel=dict(bgcolor='rgba(255, 255, 255, 0)'), selector=dict(mode='markers'), marker_colorbar=dict(tickcolor='white'))#, tickfont=dict(color='white')))
    fig.update_polars(radialaxis_range=[90, 0], bgcolor='#3a3a3a', radialaxis=dict(linecolor='white', color='white'),
                      angularaxis=dict(linecolor='white', color='white'))
    fig.update_layout(title={"text": "Azimuth vs. Elevation", "x": 0.5, "font": {"color": "white"}},  # midway
                      paper_bgcolor='#3a3a3a',
                      plot_bgcolor='#3a3a3a'
)
    fig.update_coloraxes(colorbar=dict(
    orientation='v',
    x = 1,
    title = "RSSI",
    tickcolor='white',
    title_font=dict(color='white'),
    tickfont=dict(color='white'),
))
    fig.update_coloraxes(colorbar_tickcolor='white')
    #fig.update_layout(coloraxis_showscale=False)
 
    return fig
 
#----------------------------------------------------------------------------
#another callback - for the tracking stage plot
#where we want the output to go (rssi-plot),
#and what type it is (figure)
 
@app.callback(
        Output('rssi-time-plot', 'figure'),
        [Input('polar-plot', 'clickData'),
         State('stored-data', 'children')])
 
def update_rssi_time_plot(clickData, jsonified_cleaned_data):
    if not clickData or not jsonified_cleaned_data:
        return dash.no_update
    df = pd.read_json(jsonified_cleaned_data, orient='split')
    clicked_point = clickData['points'][0]
    custom_data = clicked_point.get('customdata', [])
    contact_id = custom_data[0] if len(custom_data) > 0 else None
    if contact_id:
        filtered_df = df[df['CONTACT_ID_SATELLITE_GATEWAYSEC'] == contact_id]
        filtered_df = filtered_df.sort_values('TIMESTAMP')

        color_scale = 'Viridis_r'

        #heheheh i was right gpt was wrong i CAN do it this way
        fig = px.scatter(filtered_df, x='TIMESTAMP', y='RSSI', color = 'AZPOSSRSF',color_continuous_scale= color_scale)
        fig.update_layout(coloraxis=dict(cmin=max(df['AZPOSSRSF']), cmax=min(df['AZPOSSRSF'])))
 
        fig.update_layout(title={"text": "RSSI Elevation, Azimuth over Contact Time", "x": 0.5, "font": {"color": "white"}},  
                      paper_bgcolor='#3a3a3a',
                      plot_bgcolor='#3a3a3a')
        fig.add_trace(
            go.Scatter(x=filtered_df['TIMESTAMP'], y=filtered_df['ELPOSSRSF'], name='Elevation',
                       yaxis='y2', line=dict(color='red'))
        )
        #fig.update_traces(colorscale='Viridis_r')
        #whyis changing colorbar settings so difficult
        fig.update_coloraxes(colorbar=dict(
            orientation='h',
            x=0.5,
            y = -0.55,
            xanchor='center',
            yanchor='bottom',
            title = "Azimuth",
            tickcolor='white',
            #reversescale=True,
            title_font=dict(color='white'),
            tickfont=dict(color='white'),
        ))
        #second y axis for elv
        fig.update_layout(
            yaxis2=dict(title='Elevation', overlaying='y', side='right'),
            xaxis_title='Time',
            yaxis_title='RSSI',
            legend=dict(x = 1.3, y = 1, xanchor= 'right', yanchor = 'top', font=dict(color="white")),
            paper_bgcolor='#3a3a3a',
            plot_bgcolor='#3a3a3a')
        #prettiness
        fig.update_traces(marker=dict(size=3), hoverlabel=dict(bgcolor='rgba(255, 255, 255, 0.5)'), selector=dict(mode='markers'), marker_colorbar=dict(tickcolor='white'))#, tickfont=dict(color='white')))
 
        fig.update_xaxes(showgrid=False,tickcolor='white', tickfont=dict(color='white'))
        fig.update_yaxes(showgrid=False, tickcolor='white', tickfont=dict(color='white'))
        fig.update_yaxes(title_font_color="white")
        fig.update_xaxes(title_font_color="white")
 
        return fig
 
    return dash.no_update
 
#----------------------------------------------------------------------------
#another callback - from chatgpt 4 this time :)
@app.callback(
    [Output('data-table', 'columns'), Output('data-table', 'data')],
    [Input('polar-plot', 'clickData'), Input('stored-data', 'children')],
    [State('data-table', 'data')]  # To check current state of the data table
)
def update_table(clickData, jsonified_cleaned_data, current_data):
    df = pd.read_json(jsonified_cleaned_data, orient='split')
 
    # If polar plot is clicked, filter the table
    if clickData and current_data:
        clicked_point = clickData['points'][0]
        custom_data = clicked_point.get('customdata', [])
        contact_id = custom_data[0] if len(custom_data) > 0 else None
 
        if contact_id:
            df = df[df['CONTACT_ID_SATELLITE_GATEWAYSEC'] == contact_id]
            df = df.drop_duplicates(subset=['CONTACT_ID_SATELLITE_GATEWAYSEC'])
 
    # If no click data or initial load, show unique contact IDs
    else:
        df = df.drop_duplicates(subset=['CONTACT_ID_SATELLITE_GATEWAYSEC'])
 
    selected_columns = ['CONTACT_ID_SATELLITE_GATEWAYSEC','SRSF_START_TIME', 'SRSF_END_TIME', 'GNID', 'SAPID',
                        'SATELLITE_ID', 'CATEGORIZATION_N', 'AVG_RSSI', 'MEAPROFILE', 'TRACKINGMODE', 'ALLOCATIONTIME', 'RSSI_DROPS', 'RSSI_MIN', 'RSSI_MAX', 'RSSI_VARIANCE']
    df = df[selected_columns]
    columns = [{"name": col, "id": col} for col in df.columns]
    data = df.to_dict('records')
 
    return columns, data
#this is the callback function for displaying the data we click on
@app.callback(
    Output('clicked-data-display', 'children'),
    [Input('polar-plot', 'clickData')])
 
def display_click_data(clickData):
    if clickData is None:
        return html.P('Click on the Polar Plot to see selected datapoints', style={'font-weight': 'bold', 'color': 'white'})
    else:
        clicked_point = clickData['points'][0]
        custom_data = clicked_point.get('customdata', [])
        contact_id = custom_data[0]
        gn = custom_data[1]
        print(custom_data)
        satellite_id = custom_data[3]
        categorization = custom_data[7]
        elevation = clicked_point.get('r', 'N/A')
        azimuth = clicked_point.get('theta', 'N/A')
 
        #formatting
        return html.Div([
            html.P('Selected Datapoints:', style={'font-weight': 'bold', 'color': 'white'}),
            html.Ul([
                html.Li(f"Contact ID: {contact_id}"),
                html.Li(f"GN: {gn}"),
                html.Li(f"Satellite ID: {satellite_id}"),
                html.Li(f"Categorization: {categorization}"),
            ], style={'padding-left': '20px', 'color': 'white'})
        ])

if __name__ == '__main__': 
    app.run_server(debug=True, host='0.0.0.0', port=8050)
