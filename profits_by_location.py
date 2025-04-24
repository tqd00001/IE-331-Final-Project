

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return mo, pl, px


@app.cell
def _(pl):
    products = pl.read_parquet("pipeline/products.parquet")
    products = (
             products
            .with_columns(
                 Product_Profit = pl.col("Product_Price")-pl.col("Product_Cost")
            )
        )
    return (products,)


@app.cell
def _(pl):
    sales = pl.read_parquet("pipeline/sales.parquet")
    return (sales,)


@app.cell
def _(pl):
    stores = pl.read_parquet("pipeline/stores.parquet")
    return (stores,)


@app.cell
def _(mo):
    mo.md(
        r"""
        #Question 6
        ##How does store location affect profits? Does being downtown in a more populated area increase profits?
        """
    )
    return


@app.cell
def _(pl, products, sales, stores):
    profit = products.join(sales, on="Product_ID")
    profit_by_location = profit.join(stores, on="Store_ID")
    profit_by_location = (
            profit_by_location
            .with_columns(
               (pl.col("Product_Profit")*pl.col("Units")).alias("Sale_Profit")
            )
        )
    profit_by_location = (
            profit_by_location
            .group_by("Store_Location")
            .agg(
                 pl.col("Sale_Profit").sum().alias("Total_Location_Profit")
                )
            .sort("Total_Location_Profit", descending = True)
        )
    return (profit_by_location,)


@app.cell
def _(profit_by_location, px):
    px.bar(profit_by_location, x = "Store_Location", y = "Total_Location_Profit", title = "Profit Earned by Store Location", labels = {"Total_Location_Profit": "Profit ($)", "Store_Location": "Store Location"})
    return


@app.cell
def _(mo):
    mo.md(r"""Profits vary greatly between store locations. Downtown locations earn the most profit making approximately 2.25 million dollars, while the other three store locations don't even crest 1 million dollars in total profit. Being in more populated areas such as downtown locations is certainly increasing profits. To increase profits, targeting other locations may be a smart business decision, for example, focusing in on creating more commerical business customers to increase those profits.""")
    return


if __name__ == "__main__":
    app.run()
