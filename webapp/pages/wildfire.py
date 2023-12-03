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

register_page(__name__, path="/wildfire")

main_page = html.Div(
    className="row app-body",
    children = [
    ]
)

layout = html.Div(
    [
        # navbar,
        main_page,
        dcc.Store(id='meta-info-pr')
    ]
)
