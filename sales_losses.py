

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return (pl,)


@app.cell
def _(pl):
    products = pl.read_csv("team-01/data/products.csv")
    products
    return


@app.cell
def _(pl):
    inventory = pl.read_csv("team-01/data/inventory.csv")

    out_of_stock = inventory.filter(pl.col("Stock_On_Hand") == 0)
    out_of_stock, inventory
    return


@app.cell
def _(pl):
    sales = pl.read_csv("team-01/data/sales.csv")
    sales = sales.with_columns(
        pl.col("Date").cast(pl.Date)
    )

    sales
    return (sales,)


@app.cell
def _(pl):
    stores = pl.read_csv("team-01/data/stores.csv")
    stores = stores.with_columns(
        pl.col("Store_Open_Date").cast(pl.Date)
    )
    stores
    return (stores,)


@app.cell
def _(sales, stores):
    stores_and_sales = stores.join(sales, on="Store_ID")
    stores_and_sales = stores_and_sales.sort("Store_ID")
    stores_and_sales = stores_and_sales.drop(["Store_Open_Date"])
    stores_and_sales
    return


if __name__ == "__main__":
    app.run()
