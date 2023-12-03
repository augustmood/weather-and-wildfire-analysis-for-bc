# Import packages
from dash import Dash, html, dash_table, dcc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# from read_wildfire import wildfire_list_df
# Incorporate data
external_stylesheets = ["wildfire_style.css"]
wildfire = pd.read_csv("wildfire.csv")
wildfire_list = wildfire[["fire_num", "fire_sz_ha", "load_date", "fire_stat", "coordinate"]]

# The whole Wildfire List

dtable_df = wildfire_list.rename(columns={"fire_num": "Fire Number", "fire_sz_ha": "Fire Size (Ha)", "load_date": "Last Updated Date", "fire_stat": "Stage of Control", "coordinate": "Fire Coordinate"})

# Initialize the app
app = Dash(__name__, external_stylesheets=external_stylesheets)

# The Whole Data table
dtable = dash_table.DataTable(
    data=dtable_df.to_dict('records'),
    columns=[{"name": i, "id": i} for i in dtable_df.columns],
    sort_action="native",
    style_cell={'textAlign':'left', 'minHeight':'50px', 'maxHeight':'50px'},
    style_table={"overflowX" : "auto", "width":"1080px", "margin": "auto"},
    filter_action='native',
    style_as_list_view=True,
    style_header={
        'backgroundColor': 'rgb(255, 255, 255)',
        'height': '50px',
        'font-size': '14px',
        'font-weight': 'bold'
    },
    page_size=25,
)

# The Pie chart
bar_df = wildfire_list['fire_stat'].value_counts()
pie_df = wildfire_list['fire_stat'].value_counts(normalize=True) * 100

color_scale = px.colors.qualitative.Plotly

bar_fig = dcc.Graph(
        id='bar-chart',
        figure={
            'data': [dict(x=pie_df.index, y=pie_df, type='bar', marker=dict(color=color_scale)),],
            'layout': go.Layout(title='Fire Control Stage', xaxis=dict(title='Stage of Control'), yaxis=dict(title='Count')),
        },
        style={'float':'left', 'width':'500px'}
    )

pie_fig = dcc.Graph(
        id='pie-chart',
        figure={
            'data': [dict(labels=pie_df.index, values=pie_df, type='pie')],
            'layout': go.Layout(title='Fire Control Stage'),
        },
        style={'float':'left', 'width':'500px'}
    )

# Stage of Control Graphs
stage = html.Div([
    bar_fig,
    pie_fig,
    html.Div(style={'clear': 'both'})
], style={"margin":"auto", "width": "fit-content"})

# App layout
app.layout = html.Div([
    html.Div(children='Wildfire List'),
    dcc.Tabs([
        dcc.Tab(label="Wildfire List", children=[
            dtable
        ]),
        dcc.Tab(label="Wildfire Statistics", children=[
            stage
        ]),
    ])
])

# Run the app
if __name__ == '__main__':
    app.run(debug=True)