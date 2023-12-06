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

register_page(__name__, path="/wildfire_graphs")

# from read_wildfire import wildfire_list_df
# Incorporate data
external_stylesheets = ["style.css"]
wildfire = pd.read_csv("wildfire.csv")
wildfire_list = wildfire[["fire_num", "fire_sz_ha", "load_date", "fire_stat", "coordinate"]]

# Color Scale
color_scale = px.colors.qualitative.Plotly

################################################################################
# Stage of Control: Pie Chart
pie_df = wildfire_list['fire_stat'].value_counts(normalize=True) * 100
pie_fig = dcc.Graph(
        id='pie-chart',
        figure={
            'data': [dict(labels=pie_df.index, values=pie_df, type='pie')],
            'layout': go.Layout(title='Fire Control Stage'),
        },
        style={'float':'left', 'width':'500px'}
    )

# Stage of Control: Bar Chart
bar_df = wildfire_list['fire_stat'].value_counts()
bar_fig = dcc.Graph(
        id='bar-chart',
        figure={
            'data': [dict(x=pie_df.index, y=pie_df, type='bar', marker=dict(color=color_scale)),],
            'layout': go.Layout(title='Fire Control Stage', xaxis=dict(title='Stage of Control'), yaxis=dict(title='Count')),
        },
        style={'float':'left', 'width':'500px'}
    )

# Stage of Control Graphs
stage = html.Div([
    bar_fig,
    pie_fig,
    html.Div(style={'clear': 'both'})
], style={"margin":"auto", "width": "fit-content"})

################################################################################
## Analysis By Month
# wildfire_by_month = wildfire_list
# wildfire_by_month['load_date'] = pd.to_datetime(wildfire_by_month['load_date'])


wildfire_by_month = wildfire_list
wildfire_by_month['Month'] = wildfire_list['load_date']\
    .apply(lambda x: calendar.month_name[int(x.split('-')[1])])
wildfire_by_month = wildfire_by_month.groupby('Month').agg({'fire_sz_ha': 'sum', 'fire_num': 'count'}).reset_index()
all_months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
all_months_df = pd.DataFrame({'Month': all_months})
wildfire_by_month = pd.merge(all_months_df, wildfire_by_month, on='Month', how='left').fillna(0)
wildfire_bm_df = wildfire_by_month.rename(columns={"fire_num": "Number of Fire", "fire_sz_ha": "Fire Size Total (Ha)", "Month": "Month"})

dtable_month = dash_table.DataTable(
    data=wildfire_bm_df.to_dict('records'),
    columns=[{"name": i, "id": i} for i in wildfire_bm_df.columns],
    sort_action="native",
    style_cell={'textAlign':'left', 'minHeight':'50px', 'maxHeight':'50px'},
    style_table={"overflowX" : "auto", "width":"1080px", "margin": "auto"},
    style_as_list_view=True,
    style_header={
        'backgroundColor': 'rgb(255, 255, 255)',
        'height': '50px',
        'font-size': '14px',
        'font-weight': 'bold'
    },
    page_size=25,
)

# Fire Size by Month: Pie Chart
fire_sz_pie = dcc.Graph(
        id='pie-chart',
        figure={
            'data': [dict(labels=wildfire_by_month['Month'], values=wildfire_by_month['fire_sz_ha'], type='pie', rotation=150),],
            'layout': go.Layout(title='Fire Size by Month'),
        },
        style={'float':'left', 'width':'450px', 'margin-bottom':'50px'}
    )

# Fire Size by Month: Bar Chart
fire_sz_bar = dcc.Graph(
        id='bar-chart',
        figure={
            'data': [dict(x=wildfire_by_month['Month'], y=wildfire_by_month['fire_sz_ha'], type='bar', marker=dict(color=color_scale)),],
            'layout': go.Layout(title='Fire Size by Month', yaxis=dict(title='Count')),
        },
        style={'float':'right', 'width':'450px', 'margin-bottom':'50px'}
    )

# Fire Size by Month Graphs
fire_sz_graph = html.Div([
    fire_sz_pie,
    fire_sz_bar,
    html.Div(style={'clear': 'both'})
], style={"margin":"auto", "width": "fit-content"})

# Number of Fire by Month: Pie Chart

fire_num_pie = dcc.Graph(
        id='pie-chart',
        figure={
            'data': [dict(labels=wildfire_by_month['Month'], values=wildfire_by_month['fire_num'], type='pie', rotation=200)],
            'layout': go.Layout(title='Number of Fire by Month'),
        },
        style={'float':'left', 'width':'450px', 'margin-bottom':'50px'}
    )

# fire_num_pie_fig = go.Figure(data=[go.Pie(labels=wildfire_by_month['Month'], values=wildfire_by_month['fire_num'], hole=0.5)])
# fire_num_pie_fig.update_layout(title='Donut-Like Pie Chart')
# fire_num_pie = dcc.Graph(
#         id='donut-like-pie-chart',
#         figure=fire_num_pie_fig
#     )


# Number of Fire by Month: Bar Chart
fire_num_bar = dcc.Graph(
        id='bar-chart',
        figure={
            'data': [dict(x=wildfire_by_month['Month'], y=wildfire_by_month['fire_num'], type='bar', marker=dict(color=color_scale)),],
            'layout': go.Layout(title='Number of Fire by Month', yaxis=dict(title='Count')),
        },
        style={'float':'right', 'width':'450px', 'margin-bottom':'50px'}
    )

# Number of Fire by Month Graphs
fire_num_graph = html.Div([
    fire_num_pie,
    fire_num_bar,
    html.Div(style={'clear': 'both'})
], style={"margin":"auto", "width": "1000px"})


monthly_graph = html.Div([
    dcc.RadioItems(
        id='radio-selector',
        options=[
            {'label': 'Show Fire Size', 'value': 'fire_sz'},
            {'label': 'Show Number of Fire', 'value': 'fire_num'},
        ],
        value='fire_sz',  # Default value
        labelStyle={'display': 'block'},
        style={'float': 'right', 'margin': '10px'}
    ),
    html.Div(style={'clear': 'both'}),
    html.Div(id='graph-container')
],
style={'width':'fit-content', 'margin': 'auto'})

# Callback to update the table based on the selected radio item
@callback(
    dash.dependencies.Output('graph-container', 'children'),
    [dash.dependencies.Input('radio-selector', 'value')]
)
def update_table(selected_graph):
    if selected_graph == 'fire_sz':
        return fire_sz_graph
    elif selected_graph == 'fire_num':
        return fire_num_graph
    else:
        return html.Div("No graph to display.")

tab_height = '30px'
wildfire_stats_page = dcc.Tabs(
                children=[
                    dcc.Tab(label='Monthly Data',
                            children=[monthly_graph,
                                      html.Div([dtable_month])],
                            style={'padding': '0', 'height':tab_height, 'line-height': tab_height, 'margin-bottom': '30px'},
                            selected_style={'padding': '0', 'height':tab_height, 'line-height': tab_height, 'margin-bottom': '30px'}
                            ),
                    dcc.Tab(label='Current Fire Status', 
                            children=[stage],
                            style={'padding': '0', 'height':tab_height, 'line-height': tab_height,},
                            selected_style={'padding': '0', 'height':tab_height, 'line-height': tab_height,},
                            )],
                style={'height':tab_height, 'borderRight': '1px solid #d6d6d6'},
                )

layout = html.Div(children=[wildfire_stats_page], style={'min-width':'100%'})