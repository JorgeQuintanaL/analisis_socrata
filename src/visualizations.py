import os
import pandas as pd
from datetime import datetime as dt

import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html

import plotly.graph_objs as go

from db import MariaDB_Connect
from sqlalchemy.sql import text

db_user = "admin"
db_password = "PruebA01*"
db_host = "mariadb.cmxl0deicysi.us-east-1.rds.amazonaws.com"
db_database = "MDA"

db_connection = MariaDB_Connect(user=db_user,
                                password=db_password,
                                host=db_host,
                                database=db_database)
db_connection.connect_db()
#query = "CALL generate_synthetic_data("{}", "{}");".format("2018-10-15", "2018-10-31")
#db_connection.connection.execute(text(query).execution_options(autocommit=True))
#query = "SELECT DISTINCT fecha_ejecucion FROM test;"
# query = "SELECT COUNT(*) FROM test;"

nuevos_dataset = """
SELECT
	fecha_ejecucion,
	round(total, 0) AS total,
	round(actualizados, 0) AS actualizados,
	round(nuevos, 0) AS nuevos
FROM nuevos_actualizados;
"""
nuevos_categoria = """
SELECT
	categoria,
	fecha_ejecucion,
	round(total, 0) AS total,
	round(actualizados, 0) AS actualizados,
	round(nuevos, 0) AS nuevos
FROM nuevos_actualizados_categoria;
"""
data_nuevos = db_connection.connection.execute(nuevos_dataset).fetchall()
data_nuevos = pd.DataFrame(data_nuevos, columns=["fecha_ejecucion", "total", "actualizados", "nuevos"])

data_categoria = db_connection.connection.execute(nuevos_categoria).fetchall()
data_categoria = pd.DataFrame(data_categoria, columns=["categoria", "fecha_ejecucion", "total", "actualizados", "nuevos"])
db_connection.close_db()

colors = {
    "background": "#E3ECF9",
    "text": "#3C64AD"
}

markdown_text = """
### Dash and Markdown

Dash apps can be written in Markdown.
Dash uses the [CommonMark](http://commonmark.org/)
specification of Markdown.
Check out their [60 Second Markdown Tutorial](http://commonmark.org/help/)
if this is your first introduction to Markdown!
"""

external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]

nuevos = go.Bar(x = data_nuevos.fecha_ejecucion,
                y = data_nuevos.nuevos,
                type = "bar",
                name = "Datasets Nuevos")

actualizados = go.Bar(x = data_nuevos.fecha_ejecucion,
                      y = data_nuevos.actualizados,
                      type = "bar",
                      name = "Datasets Actualizados")

nuevos_categoria = go.Scatter(x = data_categoria.loc[data_categoria.categoria=="Ciencia, Tecnologia e Innovacion", "fecha_ejecucion"],
                              y = data_categoria.loc[data_categoria.categoria=="Ciencia, Tecnologia e Innovacion", "nuevos"],
                              name = "Nuevos")

actualizados_categoria = go.Scatter(x = data_categoria.loc[data_categoria.categoria=="Ciencia, Tecnologia e Innovacion", "fecha_ejecucion"],
                                    y = data_categoria.loc[data_categoria.categoria=="Ciencia, Tecnologia e Innovacion", "actualizados"],
                                    name = "Actualizados")

fig = go.Figure(data = [nuevos_categoria, actualizados_categoria])
fig_layout = go.Layout(plot_bgcolor = colors["background"],
                        margin=go.layout.Margin(l=10, r=10, t=5, b=5, pad=4),
                        barmode="stack",
                        paper_bgcolor = colors["background"],
                        legend=go.layout.Legend(orientation = "h"))

opts = data_categoria.groupby("categoria").count().index.tolist()
opts = [{"label": i, "value": i} for i in opts]

dates = data_categoria.groupby("fecha_ejecucion").count().index.tolist()
date_mark = {i : dates[i] for i in range(len(dates))}

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(
    style={"backgroundColor": colors["background"],
            "width": "80%",
            "margin": "auto",
            "border-radius": "15px",
            "margin-top": "20px"},
    children=[
        html.H1(
            children="Análisis Socrata",
            style = {
                "textAlign": "center",
                "color": colors["text"],
                "padding": "20px"}
        ),
        html.H4(children="Gráfica Nº 1: Tendencia de creación y actualización de datasets",
                style={"textAlign": "center",
                        "color": colors["text"],
                        "font-size": "1.5em",
                        "margin": "5px",
                        "font-weight": "light"}),
        dcc.Graph(
            id="example-graph-2",
            figure=go.Figure(data = [nuevos, actualizados],
            layout = go.Layout(plot_bgcolor = colors["background"],
                                margin=go.layout.Margin(l=50, r=50, t=5, b=5, pad=4),
                                barmode="stack",
                                paper_bgcolor = colors["background"],
                                legend=go.layout.Legend(orientation = "h")))
            ),
            html.H4(children="Tabla Nº 1: Datos desagregados por fecha de ejecución, cantidad total de datasets, nuevos y actualizados",
                style={"textAlign": "center",
                        "color": colors["text"],
                        "font-size": "1.5em",
                        "margin": "5px",
                        "font-weight": "light"}),
            	html.Div(children=[
            		html.Div(dash_table.DataTable(id="tabla_nuevos",
            						columns=[{"name": i, "id": i} for i in data_nuevos.columns],
            						data=data_nuevos.to_dict("records"),
            						fixed_rows={ "headers": True, "data": 0 },
    								style_cell={"width": "150px"},
    								style_table={"height": "350px",
                                                 "overflowY": "scroll",
    											 "overflowX": "scroll"},),
                        style={"width": "50%",
                              "height": "95%",
                              "float": "right",
                              "display": "inline-block"}),
            		html.Div(dash_table.DataTable(id="tabla_categoria",
            							columns=[{"name": i, "id": i} for i in data_categoria.columns],
            							data=data_categoria.to_dict("records"),
            							fixed_rows={ "headers": True, "data": 0 },
    									style_cell={"width": "150px"},
    									style_table={"height": "350px",
                                                     "overflowY": "scroll",
                                                     "overflowX": "scroll",},),
                        style={"width": "50%",
                               "height": "95%",
                               "float": "right",
                               "display": "inline-block"})
						],
                    style={"height": "400px", "margin-bottom": "10px"}),
        html.Div(children=[
        	html.Div(
        		html.H4(children="Gráfica Nº 2: Tendencia de creación y actualización de datasets",
        		style={"textAlign": "center",
                "color": colors["text"],
                "font-size": "1.5em",
                "margin": "auto",
                "font-weight": "light"})),
        html.Div(children=[
            html.Div(
              html.P([
                  html.Label("Categorias"),
                  dcc.Dropdown(id = "opt",
                               options = opts,
                               value = opts[0]["value"])],
                    style={"width": "80%", "margin": "10px"}),
            style = {"heigth": "200px",
                    "width": "50%",
                    "font-size": "1em",
                    "color": colors["text"],
                    "margin": "auto",
                    "float": "left",
                    "display": "inline-block"}),
            html.Div(
              html.P([
                  html.Label("Periodo de Tiempo"),
                  dcc.DatePickerRange(id="date-picker-range",
                                      min_date_allowed=pd.to_datetime("2019-10-17", format="%Y-%m-%d"),
                                      max_date_allowed=pd.to_datetime("2019-10-31", format="%Y-%m-%d"),
                                      initial_visible_month=pd.to_datetime("2019-10-20", format="%Y-%m-%d"),
                                      start_date=pd.to_datetime("2019-10-20", format="%Y-%m-%d"),
                                      end_date=pd.to_datetime("2019-10-31", format="%Y-%m-%d"))
                                ], style={"width": "80%", "margin": "10px"}),
              style = {"heigth": "200px",
                      "width": "50%",
                      "font-size": "1em",
                      "color": colors["text"],
                      "margin": "auto",
                      "float": "right",
                      "display": "inline-block"})
        ], style={"height": "100px"}),
          html.Div(dcc.Graph(id="example-graph-3", figure=fig))
          ])
    ]
)

@app.callback(dash.dependencies.Output(component_id="example-graph-3", component_property="figure"),
                                        [dash.dependencies.Input(component_id="opt", component_property="value"),
                                        dash.dependencies.Input(component_id="date-picker-range", component_property="start_date"),
                                        dash.dependencies.Input(component_id="date-picker-range", component_property="end_date")])
def update_figure(input1, start_date, end_date):
    if start_date and end_date is not None:
        aux = data_categoria.iloc[[pd.to_datetime(item) >= pd.to_datetime(start_date) and pd.to_datetime(item) <= pd.to_datetime(end_date) for item in data_categoria.fecha_ejecucion]]
        aux = aux.loc[aux.categoria == input1, :]
        nuevos_categoria = go.Scatter(x = aux.fecha_ejecucion,  y = aux.nuevos, name = "Nuevos - {}".format(input1))
        actualizados_categoria = go.Scatter(x = aux.fecha_ejecucion, y = aux.actualizados, name = "Actualizados - {}".format(input1))
        fig = go.Figure(data = [nuevos_categoria, actualizados_categoria], layout=fig_layout)
        return fig

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0")
