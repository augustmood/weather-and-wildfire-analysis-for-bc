import plotly.express as px
import yaml
import locale
import webbrowser
import sys
from dash import dcc, html, callback, Input, Output
from dash_labs.plugins import register_page
from src.data_provider import DataExtractor
sys.path.append('./')

register_page(__name__, path="/wildfire_map")


wildfire = DataExtractor().fetch_wildfire()
wildfire_list = wildfire[["fire_num", "fire_sz_ha", "load_date", "fire_stat", "coordinate"]]
with open('./config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Wildfire Map
locale.setlocale(locale.LC_NUMERIC, 'en_US.UTF-8')
map_data = wildfire
px.set_mapbox_access_token(config['TOKEN'])
map_fig = px.scatter_mapbox(map_data, lat="latitude", lon="longitude", color='fire_stat', hover_name="fire_num",
                  color_continuous_scale=px.colors.cyclical.IceFire, zoom=5, 
                  custom_data=['fire_link'])

wildfire_map = dcc.Graph(id='scatter-map', figure=map_fig,  style={'width': '90%', 'height': '700px'})

layout = html.Div(children=[wildfire_map])
