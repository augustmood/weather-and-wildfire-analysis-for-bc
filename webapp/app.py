from distutils.log import debug
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html, State
import plotly.express as px
from dash_labs.plugins import register_page
from pages import weather_table, weather_map, wildfire_list, wildfire_graphs, wildfire_map
import plotly.graph_objects as go

register_page(__name__,path="/")
app = dash.Dash(
    __name__,external_stylesheets=[dbc.themes.BOOTSTRAP]
)
fig = go.Figure()

# Define the layout of the app
# the style arguments for the sidebar. We use position:fixed and a fixed width
SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}

# the styles for the main content position it to the right of the sidebar and
# add some padding.
CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}

fig.add_layout_image(
    dict(
        source="https://www.facebook.com/photo/?fbid=196326139284532&set=a.196326119284534.png",
        xref="paper", yref="paper",
        x=1, y=1.05,
        sizex=0.2, sizey=0.2,
        xanchor="right", yanchor="bottom"
    )
)



submenu_1 = [
    html.Div(
        # use Row and Col components to position the chevrons
        html.Div(    #Screen is split in rows and columns. This is the first row with 2 columns
            children=[
                html.Div("Weather", style={'widht': '6rem', 'float': 'left', 'cursor' : 'default'}),
                html.Div(
                    html.Img(src='/assets/arrow-down-sign-to-navigate.png', style={'width': '1em','height': '1em', 'cursor' : 'pointer'}), style={'float': 'right'} #-down was -right. mr-X, X is the position
                ),
                html.Div(style={"clear": "both"})
            ],
        ),
        id="submenu-1",
        style={"width":"12.5rem", "margin":"auto", "margin-bottom":"5px"}
    ),

    dbc.Collapse(
        [
            dbc.NavLink("Table", href="/weather_table"),
            dbc.NavLink("Map", href="/weather_map"),
        ],
        id="submenu-1-collapse"
    ),
]

submenu_2 = [
html.Div(
        # use Row and Col components to position the chevrons
        html.Div(    #Screen is split in rows and columns. This is the first row with 2 columns
            children=[
                html.Div("Wildfire", style={'widht': '6rem', 'float': 'left', 'cursor' : 'default'}),
                html.Div(
                    html.Img(src='/assets/arrow-down-sign-to-navigate.png', style={'width' : '1em', 'height': '1em', 'cursor' : 'pointer'}), style={'float': 'right'} #-down was -right. mr-X, X is the position
                ),
                html.Div(style={"clear": "both"})
            ],
        ),
        id="submenu-2",
        style={"width":"12.5rem", "margin":"auto", "margin-bottom":"5px"}
    ),

    dbc.Collapse(
        [
            dbc.NavLink("Table", href="/wildfire_list"),
            dbc.NavLink("Graphs", href="/wildfire_graphs"),
            dbc.NavLink("Map", href="/wildfire_map"),
        ],
        id="submenu-2-collapse"
    ),
]


navbar = html.Div(
    [
        html.A(
            [
                html.Img(src='/assets/Logo.png', className='header-image',
                         style={'width': '100%', 'textAlign': 'center'})
            ],href='/home'
        ),
        html.Div(
            "BC Weahther and Wildfire Visualizer", className="h5",
            style={"width":"10rem", "margin": "auto"}
        ),
        html.Hr(),
        html.P(
            "", className="lead"
        ),
        dbc.Nav(submenu_1 + submenu_2, vertical=True, style={"width":"14rem", "margin": "auto"}),
    ],
    style=SIDEBAR_STYLE,
    id="sidebar",
)

content = html.Div(id="page-content", style=CONTENT_STYLE)

app.layout = html.Div([dcc.Location(id="url"), navbar, content])


def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

def set_navitem_class(is_open):
    if is_open:
        return "open"
    return ""

for i in [1, 2]:
    app.callback(
        Output(f"submenu-{i}-collapse", "is_open"),
        [Input(f"submenu-{i}", "n_clicks")],
        [State(f"submenu-{i}-collapse", "is_open")],
    )(toggle_collapse)

    app.callback(
        Output(f"submenu-{i}", "className"),
        [Input(f"submenu-{i}-collapse", "is_open")],
    )(set_navitem_class)


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])

def render_page_content(pathname):
    if pathname == "/":
        pathname = '/weather_map'
        return render_page_content(pathname)
    elif pathname == '/weather_table':
        return weather_table.layout
    elif pathname == '/weather_map':
        return weather_map.layout
    elif pathname == '/wildfire_list':
        return wildfire_list.layout
    elif pathname == '/wildfire_graphs':
        return wildfire_graphs.layout
    elif pathname == '/wildfire_map':
        return wildfire_map.layout
    # If the user tries to reach a different page, return a 404 message
    return html.Div(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ],
        className="p-3 bg-light rounded-3",
    )

if __name__ == "__main__":
    app.run_server(port=8080,debug=False)
