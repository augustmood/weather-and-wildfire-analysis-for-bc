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

# register_page(__name__, path="/wildfire/")
# from read_wildfire import wildfire_list_df
# Incorporate data
external_stylesheets = ["style.css"]
wildfire = pd.read_csv("wildfire.csv")
wildfire_list = wildfire[["fire_num", "fire_sz_ha", "load_date", "fire_stat", "coordinate"]]

# The whole Wildfire List

dtable_df = wildfire_list.rename(columns={"fire_num": "Fire Number", "fire_sz_ha": "Fire Size (Ha)", "load_date": "Last Updated Date", "fire_stat": "Stage of Control", "coordinate": "Fire Coordinate"})

# The Whole Data table
dtable = dash_table.DataTable(
    data=dtable_df.to_dict('records'),
    columns=[{"name": i, "id": i} for i in dtable_df.columns],
    sort_action="native",
    style_cell={'textAlign':'left', 'minHeight':'50px', 'maxHeight':'50px'},
    style_table={"height": "600px", "overflowY" : "auto", "width":"1080px", 
                 "margin": "auto", "margin-top":"5px", "border": "1px #d6d6d6 solid"},
    filter_action='native',
    style_as_list_view=True,
    style_header={
        'backgroundColor': 'rgb(255, 255, 255)',
        'height': '50px',
        'font-size': '14px',
        'font-weight': 'bold'
    },
)

wildfire_list_page = html.Div(children=[dtable])