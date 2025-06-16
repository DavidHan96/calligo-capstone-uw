# main.py
from pathlib import Path
import threading
import pandas as pd
import dash
from dash import html, dcc, Output, Input
import plotly.express as px
from sklearn.metrics import classification_report
from dash import dash_table


from run_model import _run_notebook
from models.macro_model_s3 import build_forecast

app = dash.Dash(__name__)

# ---------- XGBoost static CSV ----------------------------------------------

XGB_DF = pd.read_csv(Path("app/data/lumber_xgb.csv"))
XGB_REPORT = pd.read_csv(Path("app/data/lumber_xgb_report.csv"))
#print(XGB_REPORT.head())

# ---------- Monte-Carlo static image ----------------------------------------

MC_IMAGE = "/assets/lumber_mc_hist.png"        
MC_IMAGE_CRUDE = "/assets/crude_mc_hist.png" 

# ---------- VAR notebook (sample notebook) ----------------------------------

VAR_PATH = Path("notebooks/VAR.ipynb")
var_df   = None                               

def run_var():
    global var_df
    res = _run_notebook(VAR_PATH)             
    var_df = res["df"]

threading.Thread(target=run_var, daemon=True).start()

# ---------- layout ----------------------------------------------------------

CURRENCIES = ["usd_krw", "usd_china", "usd_uk"]

app.layout = html.Div(
    [
        html.H2("Lumber Monte-Carlo (static image)"),
        html.Img(src=MC_IMAGE, style={"maxWidth": "700px"}),

        html.H2("Crude oil Monte-Carlo (static image)"),
        html.Img(src=MC_IMAGE_CRUDE, style={"maxWidth": "700px"}),

        html.H2("XGBoost Feature Importance"),
        dcc.Graph(
            id="xgb-graph",
            figure=px.bar(XGB_DF, x="Feature", y="Importance",
                          title="XGBoost Feature Importance"),
        ),

       html.H2("Classification Report"),
       dash_table.DataTable(
            id='classification-table',
            columns=[{"name": col, "id": col} for col in XGB_REPORT.columns],
            data=XGB_REPORT.to_dict('records'),
            style_cell={'textAlign': 'center', 'padding': '8px'},
            style_header={'backgroundColor': '#f4f4f4', 'fontWeight': 'bold'},
            style_table={'overflowX': 'auto'},
            style_data_conditional=[
                {
                    'if': {'column_id': 'Class'},
                    'textAlign': 'left',
                    'fontWeight': 'bold'
                }
            ]
        ),

        html.H2("VAR Forecast (KRW/USD)"),
        dcc.Graph(id="var-graph"),
        dcc.Interval(id="var-refresh", interval=2000, n_intervals=0),

        html.H2("Macro (Ridge + RF) model"),
        dcc.Dropdown(
            id="currency-picker",
            options=[{"label": c, "value": c} for c in CURRENCIES],
            value="usd_krw",
            clearable=False,
        ),
        dcc.Graph(id="macro-graph"),

        html.Footer("Calligo - Capstone"),
    ]
)

# ---------- update VAR once ready ------------------------------------------

@app.callback(
    Output("var-graph", "figure"),
    Input("var-refresh", "n_intervals"),
)
def load_var_graph(_):
    if var_df is None:
        return px.line(title="Loading VAR notebook")
    x_col = var_df.columns[0]
    return px.line(var_df, x=x_col, y=var_df.columns[1:],
                   title="VAR Forecast")

# ---------- macro dropdown --------------------------------------------------

@app.callback(
    Output("macro-graph", "figure"),
    Input("currency-picker", "value"),
)
def show_macro(currency):
    df = build_forecast(currency)
    return px.line(df, x="date", y="predicted_value",
                   title=f"{currency.upper()} 14-day Forecast")

# ---------- run -------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
