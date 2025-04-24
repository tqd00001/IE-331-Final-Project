

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import plotly as pl
    import plotly.express as px
    return (pl,)


@app.cell
def _(pl):
    sales = pl.read_parquet("pipeline/sales.parquet")
    sales
    return


if __name__ == "__main__":
    app.run()
