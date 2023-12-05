from distutils.log import debug
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
import plotly.express as px
from dash_labs.plugins import register_page
from pages import weather, wildfire_list, wildfire_graphs, wildfire_map
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

sidebar = html.Div(
        className='container',
        children=[
        # Image at the top
        html.Img(src='/assets/Logo.png', className='header-image',style={'width':'100%','textAlign': 'center' }),
        html.Hr(),
        html.P(
            "BC Weahther and Wildfire Visualizer", className="h5"
        ),
        dbc.Nav(
            [
                dbc.NavLink("Home", href="/", active="exact"),
                dbc.NavLink("Weather", href="/weather", active="exact"),
                # dbc.NavLink("Wildfire Dashboard", href="/wildfire", active="exact"),
                dbc.DropdownMenu(
                    children=[
                        dbc.DropdownMenuItem("Table", href="/wildfire_list"),
                        dbc.DropdownMenuItem("Graphs", href="/wildfire_graphs"),
                        dbc.DropdownMenuItem("Map", href="/wildfire_map"),
                    ],
                    nav=True,
                    in_navbar=True,
                    label="Wildfire Dashboard",
                ),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    style=SIDEBAR_STYLE,
)

content = html.Div(id="page-content", style=CONTENT_STYLE)

app.layout = html.Div([dcc.Location(id="url"), sidebar, content])

@app.callback(Output("page-content", "children"), [Input("url", "pathname")])

def render_page_content(pathname):
    if pathname == "/":
        return 
    elif pathname == '/weather':
        return weather.layout
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
    app.run_server(debug=False)
