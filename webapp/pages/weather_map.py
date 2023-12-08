from typing import List
import datetime
import pytz
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, callback, State
#from plotter import Plotter
import plotly.express as px
from dash_labs.plugins import register_page
from dash.exceptions import PreventUpdate
import pandas as pd
import dash_leaflet as dl
import dash_leaflet.express as dlx
from dash_extensions.javascript import arrow_function, assign

register_page(__name__, path="/weather_map")


df = pd.read_csv('current_weather.csv')
# A few cities (assuming df is defined somewhere in your code).
cities = [eval(i) for i in df.json.to_list()]
# Generate geojson with a marker for each country and name as tooltip.
geojson = dlx.dicts_to_geojson(
    [{**c, **dict(tooltip=c['city'] + ', <b>' + c['condition'] + '<b/>' + ', <b>' + str(c['temp_c'])[0:4] + '°C<b/>')} for c in cities])
# Create javascript function that draws a marker with a custom icon.
draw_flag = assign("""function(feature, latlng){
const weather_flag = L.icon({iconUrl: `./assets/64x64/${feature.properties.condition_icon_id}.png`, iconSize: [64, 64]});
return L.marker(latlng, {icon: weather_flag});
}""")

eventHandlers = dict(
    contextmenu=assign("function(e, ctx){ctx.map.flyTo([53.726669, -127.647621], 5);}"),
)


def get_info(feature=None):
    header_style = {"fontFamily": "Consolas, monaco, monospace", "text-align": "center", "fontSize": "24px", "fontWeight": "bold"}
    text_style = {"fontFamily": "Consolas, monaco, monospace", "text-align": "center", "fontSize": "18px", "margin-top":"20px"}
    line_style = {"fontFamily": "Consolas, monaco, monospace", "display": "inline-block", "fontWeight": "bold"}
    item_style = {"fontFamily": "Consolas, monaco, monospace", "display": "inline-block","float": "right"}
    header = [html.Div("Current Weather", style=header_style)]
    if not feature:
        return header + [
            html.Div(html.B('?'), style={"text-align": "center", "fontFamily": "Consolas, monaco, monospace", "fontSize": "128px"}),
            html.P("Select a city", style=text_style)]
    return [
        html.Div(feature["properties"]["city"], style=header_style), html.Br(),
        html.Div(html.Img(src=f'./assets/128x128/{feature["properties"]["condition_icon_id"]}.png'), style={"text-align": "center"}),
        html.Div(feature["properties"]["condition"], style=text_style),
        html.Hr(style={"width": "100%", "color": "#9a9a9a", "text-align": "center"}),
        html.Br(),
        html.Div([
            html.B("Temperature:", style=line_style),
            html.Div(str(feature["properties"]["temp_c"])+"°C", style=item_style)]),
        html.Div([
            html.B("Wind Speed:", style=line_style),
            html.Div(str(feature["properties"]["wind_kph"])+" km / h", style=item_style)]),
        html.Div([
            html.B("Wind Degree:", style=line_style),
            html.Div(str(feature["properties"]["wind_degree"])+"°", style=item_style)]),
        html.Div([
            html.B("Wind Direction:", style=line_style),
            html.Div(feature["properties"]["wind_dir"], style=item_style)]),
        html.Div([
            html.B("Cloud Cover:", style=line_style),
            html.Div(str(feature["properties"]["cloud"])+"%", style=item_style)]),
        html.Div([
            html.B("Humidity:", style=line_style),
            html.Div(str(feature["properties"]["humidity"])+"%", style=item_style)]),
        html.Div([
            html.B("PM 2.5:", style=line_style),
            html.Div(str(feature["properties"]["pm2_5"]), style=item_style)]),
        html.Div([
            html.B("Updated at:", style=line_style),
            html.Div(str(feature["properties"]["last_updated"]), style=item_style)])
        ]


info = html.Div(
    id='info',
    children=get_info(),
    className='info',
    style={
        'position': 'fixed',
        'top': '40px',
        'right': '60px',
        'padding-top': '40px',
        'padding-bottom': '40px',
        'padding-left': '60px',
        'padding-right': '60px',
        "width": "360px",
        'background-color': 'rgba(255, 255, 255, 0.7)',
        'color': 'black',
        'border-radius': '10px',
        'cursor': 'pointer',
        'visibility': 'visible',
        'transition': 'visibility 0s, opacity 0.5s linear',
        "display": "block",
        'zIndex': '1000'
    })

main_page = html.Div([
    html.Div([dcc.Dropdown(list(df.city), placeholder="Select a city...", id='map-city-dropdown')],
             style={"position": "fixed", "top": "40px", "left": "360px", "width": "20%", "zIndex": "100000"}),
    dl.Map(children=[
        dl.TileLayer(),
        dl.GeoJSON(data=geojson, id='geojson',
                   pointToLayer=draw_flag,
                   hoverStyle=arrow_function(dict(weight=5, color='#666', dashArray='')),
                   zoomToBounds=True),
        dl.LocateControl(locateOptions={'enableHighAccuracy': True}),
        dl.ScaleControl(position="bottomleft")
    ], style={'height': '100vh'}, eventHandlers=eventHandlers, center=[53.726669, -127.647621], zoom=5,
        id='map-container'),
    info
])


@callback(Output('map-city-dropdown', 'value'),
              Input("geojson", "clickData"))
def info_hover(feature):
    if feature is not None:
        return feature["properties"]["city"]


@callback(
    [Output('map-container', 'viewport'), Output('info', 'children')],
    [Input('map-city-dropdown', 'value')],
    prevent_initial_call=True
)
def update_map(value):
    if value is not None:
        selected_city = next((item for item in cities if item['city'] == value), None)
        return [dict(center=[selected_city['lat'], selected_city['lon']], zoom=12, transition="flyTo"),
                get_info(dict(properties=selected_city))]
    else:
        return [dict(center=[53.726669, -127.647621], zoom=5, transition="flyTo"), get_info()]



layout = html.Div(
    [
        # navbar,
        main_page,
        dcc.Store(id='meta-info')
    ]
)
