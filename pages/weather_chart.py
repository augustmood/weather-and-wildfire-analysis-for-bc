import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html, callback, Input, Output, State
from dash_labs.plugins import register_page
from plotly.subplots import make_subplots

register_page(__name__, path="/weather_chart")


df_history = pd.read_csv('./data/history_weather.csv')
df_forecast = pd.read_csv('./data/forecast_weather.csv')

color_scale = px.colors.qualitative.Plotly

def day_chart(df, city, data_type='history'):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    (fig.add_trace(
        go.Scatter(x=df.date, y=df.maxtemp_c, name="Max Temp", mode="lines+markers", marker_color='#ea5545',
                   line_color='#ea5545'),
        secondary_y=False,
    ).add_trace(
        go.Scatter(x=df.date, y=df.mintemp_c, name="Min Temp", mode="lines+markers", marker_color='#27aeef',
                   line_color='#27aeef'),
        secondary_y=False,
    ).add_trace(
        go.Scatter(x=df.date, y=df.avgtemp_c, name="Avg Temp", mode="lines+markers", marker_color='#87bc45',
                   line_color='#87bc45'),
        secondary_y=False,
    ).add_trace(
        go.Bar(x=df.date, y=df.avghumidity, name="Avg Humidity", opacity=0.5),
        secondary_y=True,
    ).update_layout(title_text=f"{city} Historical Weather in Last 7 Days")
     .update_xaxes(title_text="Date"))

    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Temperature</b> °C", secondary_y=False)
    fig.update_yaxes(title_text="<b>Humidity</b> %", secondary_y=True).update_layout(hovermode='x unified')

    if data_type == "history":
        fig.update_layout(title_text=f"{city} Historical Weather in Last 7 Days")
        count = 1
        for index, row in df.iterrows():
            fig.add_layout_image(
                dict(
                    source=f'./assets/64x64/{row.condition_icon_id}.png',
                    xref='x domain', yref='paper',
                    x=count / 7 - 0.02, y=0.96,
                    sizex=0.24, sizey=0.24,
                    xanchor='right', yanchor='bottom'
                )
            )
            count += 1
    else:
        fig.update_layout(title_text=f"{city} Forecast Weather in Next 3 days")
        count = 1
        for index, row in df.iterrows():
            fig.add_layout_image(
                dict(
                    source=f'./assets/64x64/{row.condition_icon_id}.png',
                    xref='x domain', yref='paper',
                    x=count / 3.1 - 0.1, y=0.96,
                    sizex=0.24, sizey=0.24,
                    xanchor='right', yanchor='bottom'
                )
            )
            count += 1

    return fig

def day_condition_pie(df, city, data_type='history'):
    if data_type == "history":
        title = f'{city} Weather Condition Summary in Last 7 Days'
    else:
        title = f'{city} Weather Condition Summary in Next 3 Days'
    condition_counts = df.condition.value_counts()
    fig = px.pie(values=condition_counts, names=condition_counts.index, title=title)

    return fig

def melt_history(df, city, date):
    temp_df = df[(df.city == city) & (df.date == date)]
    return pd.concat([temp_df[['is_day_at' + str(i), 'temp_c_at' + str(i), 'humidity_at' + str(i),
                               'wind_mph_at' + str(i), 'wind_degree_at' + str(i), 'wind_dir_at' + str(i),
                               'cloud_at' + str(i), 'condition_at' + str(i), 'condition_icon_id_at' + str(i)]].rename(
        columns={
            'is_day_at' + str(i): 'is_day', 'temp_c_at' + str(i): 'temp_c',
            'humidity_at' + str(i): 'humidity', 'wind_mph_at' + str(i): 'wind_mph',
            'wind_degree_at' + str(i): 'wind_degree', 'wind_dir_at' + str(i): 'wind_dir',
            'cloud_at' + str(i): 'cloud', 'condition_at' + str(i): 'condition',
            'condition_icon_id_at' + str(i): 'condition_icon_id'
        }) for i in range(24)], ignore_index=True).reset_index(drop=False).rename(columns={'index': 'hour'})


def melt_forecast(df, city, date):
    temp_df = df[(df.city == city) & (df.date == date)]
    return pd.concat([temp_df[['is_day_at' + str(i), 'temp_c_at' + str(i), 'humidity_at' + str(i),
                               'wind_mph_at' + str(i), 'wind_degree_at' + str(i), 'wind_dir_at' + str(i),
                               'cloud_at' + str(i), 'condition_at' + str(i), 'condition_icon_id_at' + str(i),
                               'chance_of_rain_at' + str(i), 'chance_of_snow_at' + str(i)]].rename(columns={
        'is_day_at' + str(i): 'is_day', 'temp_c_at' + str(i): 'temp_c',
        'humidity_at' + str(i): 'humidity', 'wind_mph_at' + str(i): 'wind_mph',
        'wind_degree_at' + str(i): 'wind_degree', 'wind_dir_at' + str(i): 'wind_dir',
        'cloud_at' + str(i): 'cloud', 'condition_at' + str(i): 'condition',
        'condition_icon_id_at' + str(i): 'condition_icon_id', 'chance_of_rain_at' + str(i): 'chance_of_rain',
        'chance_of_snow_at' + str(i): 'chance_of_snow'
    }) for i in range(24)], ignore_index=True).reset_index(drop=False).rename(columns={'index': 'hour'})


def hour_chart(df, city, date, data_type='history'):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    (fig.add_trace(
        go.Scatter(x=df.hour, y=df.temp_c, name="Temperature", mode="lines+markers", marker_color='#ea5545',
                   line_color='#ea5545'),
        secondary_y=False,
    ).add_trace(
        go.Bar(x=df.hour, y=df.humidity, name="Humidity", opacity=0.5),
        secondary_y=True,
    ).update_xaxes(title_text="Hour"))

    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Temperature</b> °C", secondary_y=False)
    fig.update_yaxes(title_text="<b>Humidity</b> %", secondary_y=True).update_layout(hovermode='x unified')
    if data_type == 'history':
        fig.update_layout(title_text=f"{city} Historical Weather in {date}")
    else:
        fig.update_layout(title_text=f"{city} Forecast Weather in {date}")

    count = 1
    for index, row in df.iterrows():
        fig.add_layout_image(
            dict(
                source=f'./assets/64x64/{row.condition_icon_id}.png',
                xref='x domain', yref='paper',
                x=count / 25.6 + 0.035, y=1,
                sizex=0.14, sizey=0.14,
                xanchor='right', yanchor='bottom'
            )
        )
        count += 1

    return fig

def hour_chart_wind_cloud(df, city, date):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    (fig.add_trace(
        go.Scatter(x=df.hour, y=df.wind_mph, name="Wind Speed", mode="lines+markers", marker_color='#27aeef',
                   line_color='#27aeef', hovertemplate='<br><b>Wind Speed</b>: %{x}<br>' + '<br>%{text}<br>',
                   text=[f"<b>Wind Direction</b>: {i}" for i in df.wind_dir]),
        secondary_y=False,
    ).add_trace(
        go.Bar(x=df.hour, y=df.cloud, name="Cloud Coverage", opacity=0.5),
        secondary_y=True,
    ).update_layout(title_text=f"{city} Wind & Cloud in {date}")
     .update_xaxes(title_text="Hour"))

    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Wind Speed</b> km/h", secondary_y=False)
    fig.update_yaxes(title_text="<b>Cloud Coverage</b> %", secondary_y=True).update_layout(hoverlabel_align='right')

    return fig

def hour_chart_rain_snow(df, city, date):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=df.hour, y=df.chance_of_rain, name="Chance of Rain", opacity=0.5)
    ).add_trace(
        go.Bar(x=df.hour, y=df.chance_of_snow, name="Chance of Snow", opacity=0.5)
    ).update_xaxes(title_text="Hour")

    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Probability</b> %", secondary_y=False)
    fig.update_layout(title_text=f"{city} Chance of Rain & Snow in {date}")
    return fig


def day_level(df, city, data_type='history'):
    temp_df = df[df.city == city]
    return day_chart(temp_df, city, data_type), day_condition_pie(temp_df, city, data_type)


def hour_level(df, city, date, data_type='history'):
    if data_type == "history":
        temp_df = melt_history(df, city, date)
        return hour_chart(temp_df, city, date, 'history'), hour_chart_wind_cloud(temp_df, city, date)
    else:
        temp_df = melt_forecast(df, city, date)
        return hour_chart(temp_df, city, date, 'forecast'), hour_chart_wind_cloud(temp_df, city, date), hour_chart_rain_snow(temp_df, city, date)

dropdown_style = {"position": "relative", "top": "20px", "left": "0px", "width": "20%", "zIndex": "100000"}
day_chart_style = {"width": "960px", "margin":"auto"}
dropdown_style2 = {"width": "200px"}

history_tab = html.Div(children=[
    html.Div([dcc.Dropdown(list(df_history.city.unique()), placeholder="Select a city...", id='history-city-dropdown')],
             style=dropdown_style),
    html.Div(children=[], id="history-city-dropdown-data")], id='history-tab')

forecast_tab = html.Div(children=[
    html.Div(
        [dcc.Dropdown(list(df_forecast.city.unique()), placeholder="Select a city...", id='forecast-city-dropdown')],
        style=dropdown_style),
    html.Div(children=[], id="forecast-city-dropdown-data")], id='forecast-tab')

weather_chart_page = dcc.Tabs(
    children=[
        dcc.Tab(label='History Weather',
                children=[history_tab],
                style={'padding': '0', 'height': '30px', 'line-height': '30px', 'margin-bottom': '30px'},
                selected_style={'padding': '0', 'height': '30px', 'line-height': '30px', 'margin-bottom': '30px'}
                ),
        dcc.Tab(label='Forecast Weather',
                children=[forecast_tab],
                style={'padding': '0', 'height': '30px', 'line-height': '30px', },
                selected_style={'padding': '0', 'height': '30px', 'line-height': '30px', },
                )],
    style={'height': '30px', 'borderRight': '1px solid #d6d6d6'},
)

@callback(Output("history-city-dropdown-data", "children"), Input("history-city-dropdown", "value"))
def history_tab_update(value):
    history_day_charts = day_level(df_history, value, data_type='history')
    history_day_charts_layout = html.Div(children=[
        html.Div(dcc.Graph(figure=history_day_charts[0], id="history-day-chart1"),
                 style=day_chart_style),
        html.Div(dcc.Graph(figure=history_day_charts[1], id="history-day-chart2"),
                 style=day_chart_style),
        html.Div(style={"clear": "both"}),
        html.Div(
            dcc.Dropdown(list(df_history.date.unique()), placeholder="Select a date...", id='history-city-day-dropdown', style=dropdown_style2),
                         style={"float":"right", "width":"fit-content"}),
        html.Div(style={"clear": "both"}),
        html.Div(children=[], id="history-city-day-dropdown-data")
    ])
    return history_day_charts_layout


@callback(Output("history-city-day-dropdown-data", "children"), Input("history-city-day-dropdown", "value"),
              State("history-city-dropdown", "value"))
def history_tab_update(value1, value2):
    history_day_charts = hour_level(df_history, value2, value1, data_type='history')
    history_day_charts_layout = html.Div(children=[
        html.Div(dcc.Graph(figure=history_day_charts[0], id="history-hour-chart1"),
                 style=day_chart_style),
        html.Div(dcc.Graph(figure=history_day_charts[1], id="history-hour-chart2"),
                 style=day_chart_style)

    ])
    return history_day_charts_layout


@callback(Output("forecast-city-dropdown-data", "children"), Input("forecast-city-dropdown", "value"))
def forecast_tab_update(value):
    forecast_day_charts = day_level(df_forecast, value, data_type='forecast')
    forecast_day_charts_layout = html.Div(children=[
        html.Div(dcc.Graph(figure=forecast_day_charts[0], id="forecast-day-chart1"),
                 style=day_chart_style),
        html.Div(dcc.Graph(figure=forecast_day_charts[1], id="forecast-day-chart2"),
                 style=day_chart_style),
        html.Div(style={"clear": "both"}),
        html.Div(
            dcc.Dropdown(list(df_forecast.date.unique()), placeholder="Select a date...", id='forecast-city-day-dropdown',
                         style=dropdown_style2),
                         style={"float":"right", "width":"fit-content"}),
        html.Div(style={"clear": "both"}),
        html.Div(children=[], id="forecast-city-day-dropdown-data")
    ])
    return forecast_day_charts_layout


@callback(Output("forecast-city-day-dropdown-data", "children"), Input("forecast-city-day-dropdown", "value"),
              State("forecast-city-dropdown", "value"))
def forecast_tab_update(value1, value2):
    forecast_day_charts = hour_level(df_forecast, value2, value1, data_type='forecast')
    forecast_day_charts_layout = html.Div(children=[
        html.Div(dcc.Graph(figure=forecast_day_charts[0], id="forecast-hour-chart1"),
                 style=day_chart_style),
        html.Div(dcc.Graph(figure=forecast_day_charts[1], id="forecast-hour-chart2"),
                 style=day_chart_style),
        html.Div(dcc.Graph(figure=forecast_day_charts[2], id="forecast-hour-chart3"),
                 style=day_chart_style)

    ])
    return forecast_day_charts_layout

layout = html.Div(children=[weather_chart_page], style={'min-width': '100%'})
