import pandas as pd
from typing import List
import datetime
import pytz
import dateutil.relativedelta
import dash
import dash_bootstrap_components as dbc
import plotly.express as px
from dash import dcc, html, callback, Input, Output, State
#from plotter import Plotter
import re
import dash_daq as daq
from dash_labs.plugins import register_page
from dash.exceptions import PreventUpdate
# Import packages
from dash import Dash, html, dash_table, dcc, Input, Output
import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import calendar
import locale
import webbrowser

# from read_wildfire import wildfire_list_df
# Incorporate data
external_stylesheets = ["style.css"]
wildfire = pd.read_csv("wildfire.csv")
wildfire_list = wildfire[["fire_num", "fire_sz_ha", "load_date", "fire_stat", "coordinate"]]


# Wildfire Map

locale.setlocale(locale.LC_NUMERIC, 'en_US.UTF-8')
map_data = wildfire
px.set_mapbox_access_token("pk.eyJ1IjoiYnJhbmRvbmxpIiwiYSI6ImNscHJrejRvbzAwcWcya2xiNDR6bHNyMDkifQ.vd0IjbvmrKvkdpFRp9FOiw")
map_fig = px.scatter_mapbox(map_data, lat="latitude", lon="longitude", color='fire_stat',
                  color_continuous_scale=px.colors.cyclical.IceFire, zoom=5,
                  custom_data=['fire_link'])
# map_fig.update_traces(cluster=dict(enabled=True))

wildfire_map = dcc.Graph(id='scatter-map', figure=map_fig,  style={'width': '90%', 'height': '700px'})
# Callback to open the URL in a new tab when a scatter marker is clicked
@callback(
    Output('scatter-map', 'selectedData'),
    [Input('scatter-map', 'clickData')]
)
def open_url_in_new_tab(click_data):
    if click_data:
        selected_url = click_data['points'][0].get('customdata', None)[0]
        if selected_url:
            print(click_data)
            webbrowser.open_new_tab(selected_url)
    return None