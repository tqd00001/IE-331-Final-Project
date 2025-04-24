

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
def _(mo):
    mo.md(r"""#Question 1""")
    return


@app.cell
def _(mo):
    mo.md(r"""##Which product categories drive the biggest profits?""")
    return


@app.cell
def _(pl):
    products = pl.read_csv("team-01/data/products.csv")
    products = (
         products
         .with_columns(
             pl.col(["Product_Cost","Product_Price"])
             .str.strip_chars("$")
             .str.strip_chars()
         )
         .select(
             pl.col("Product_ID"),
             pl.col("Product_Category"),
             pl.col("Product_Cost").cast(pl.Float32),
             pl.col("Product_Price").cast(pl.Float32)
         )
        .with_columns(
             Product_Profit = pl.col("Product_Price")-pl.col("Product_Cost")
        )
    )
    return (products,)


@app.cell
def _(pl):
    sales = pl.read_csv("team-01/data/sales.csv")
    product_sales = (
        sales
        .group_by("Product_ID")
        .agg(
            pl.col("Units").sum().alias("Units_Sold_By_Product")
        )
        .sort("Product_ID")
        )
    return (product_sales,)


@app.cell
def _(pl):
    stores = pl.read_csv("team-01/data/stores.csv")
    stores
    return


@app.cell
def _(pl, product_sales, products):
    profit_by_category = products.join(product_sales, on="Product_ID")
    profit_by_category = (
        profit_by_category
        .with_columns(
            (pl.col("Product_Profit")*pl.col("Units_Sold_By_Product")).round(2).alias("Total_Product_Profit")
            )
        .group_by("Product_Category")
        .agg(
            pl.col("Total_Product_Profit").sum()
        )
        .sort("Total_Product_Profit", descending = True)
    )
    return (profit_by_category,)


@app.cell
def _(profit_by_category, px):
    px.bar(profit_by_category, x = "Product_Category", y = "Total_Product_Profit", title = "Profit Earned by Product Category", labels = {"Product_Category": "Product Category","Total_Product_Profit": "Profit"})
    return


@app.cell
def _(mo):
    mo.md(r"""Toys are the biggest driver of profit, while the sports & outdoors category fuels profit the least out of the five product categories.""")
    return


@app.cell
def _(mo):
    mo.md(r"""##Is this the same across store locations?""")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
