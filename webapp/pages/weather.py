from typing import List
import datetime
import pytz
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, callback, State
#from plotter import Plotter
import plotly.express as px
from dash_labs.plugins import register_page
from dash.exceptions import PreventUpdate

register_page(__name__, path="/weather")

main_page = html.Div(
    className="row app-body",
    children = [
    ]
)

layout = html.Div(
    [
        # navbar,
        main_page,
        dcc.Store(id='meta-info')
    ]
)
