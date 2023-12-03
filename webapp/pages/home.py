from distutils.log import debug
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, callback
from dash.dependencies import Input, Output, State
import plotly.express as px
from dash_labs.plugins import register_page
from pages import weather,wildfire

register_page(__name__,path="/")

# Define the layout of the app
layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'
    )
])


@callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
              
def display_page(pathname=None):
    if pathname == '/home':
        return home.layout
    elif pathname == '/weather':
        return weather.layout
    elif pathname == '/wildfire':
        return wildfire.layout
    else:
        return home.layout
