import sys
from dash_labs.plugins import register_page
from dash import html, dash_table
from src.data_provider import DataExtractor
sys.path.append('./')

register_page(__name__, path="/wildfire_list")

wildfire = DataExtractor().fetch_wildfire()
wildfire_list = wildfire[["fire_num", "fire_sz_ha", "load_date", "fire_stat", "coordinate"]]

# The whole Wildfire List
dtable_df = wildfire_list.rename(columns={"fire_num": "Fire Number", "fire_sz_ha": "Fire Size (Ha)", "load_date": "Last Updated Date", "fire_stat": "Stage of Control", "coordinate": "Fire Coordinate"})

# The Whole Data table
dtable = dash_table.DataTable(
    data=dtable_df.to_dict('records'),
    columns=[{"name": i, "id": i} for i in dtable_df.columns],
    sort_action="native",
    style_cell={'textAlign':'left', 'minHeight':'50px', 'maxHeight':'50px'},
    style_table={"height": "600px", "overflowY" : "auto", "width":"1080px", 
                 "margin": "auto", "margin-top":"5px", "border": "1px #d6d6d6 solid"},
    filter_action='native',
    style_as_list_view=True,
    style_header={
        'backgroundColor': 'rgb(255, 255, 255)',
        'height': '50px',
        'font-size': '14px',
        'font-weight': 'bold'
    },
)

layout = html.Div(children=[dtable])