import dash
import dash_labs as dl
import dash_bootstrap_components as dbc
import waitress

#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', dbc.themes.LUX]
app = dash.Dash(
    __name__, plugins=[dl.plugins.pages]#, external_stylesheets=external_stylesheets
)

navbar = dbc.NavbarSimple(
    dbc.DropdownMenu(
        [
            dbc.DropdownMenuItem(page["name"], href=page["path"])
            for page in dash.page_registry.values()
            if page["module"] != "pages.not_found_404"
        ],
        nav=True,
        label="More Pages",
    ),
    brand="BC WEATHER VISUALIZER",
    brand_href="/",
    color="primary",
    dark=True,
    className="mb-2",
)

app.layout = dbc.Container(
    [navbar, dl.plugins.page_container],
    fluid=True
)

if __name__ == "__main__":
    app.run_server(debug=False)
#    waitress.serve(app.server, listen='*:8080')
