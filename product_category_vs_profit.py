

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
    return product_sales, sales


@app.cell
def _(pl):
    stores = pl.read_csv("team-01/data/stores.csv")
    stores
    return (stores,)


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
    px.bar(profit_by_category, x = "Product_Category", y = "Total_Product_Profit", title = "Profit Earned by Product Category", labels = {"Product_Category": "Product Category","Total_Product_Profit": "Profit ($)"})
    return


@app.cell
def _(mo):
    mo.md(r"""Toys are the biggest driver of profit bringing in aproximately 1.08 million dollare, while the sports & outdoors category fuels profit the least out of the five product categories making only 505.718 thousand dollars.""")
    return


@app.cell
def _(mo):
    mo.md(r"""##Is this the same across store locations?""")
    return


@app.cell
def _(pl, products, sales, stores):
    profit_category_location = products.join(sales, on="Product_ID")
    profit_category_location = profit_category_location.join(stores, on="Store_ID")
    profit_category_location = (
        profit_category_location
         .with_columns(
            (pl.col("Product_Profit")*pl.col("Units")).round(2).alias("Sale_Profit")
            )
        .select("Product_Category", "Sale_Profit", "Store_Location")
        .group_by("Product_Category","Store_Location")
        .agg(
            pl.col("Sale_Profit").sum().alias("Total_Profit")
        )
        .sort("Total_Profit", descending=True)
    )
    return (profit_category_location,)


@app.cell
def _(profit_category_location, px):
    px.bar(profit_category_location, x = "Store_Location", y = "Total_Profit", color = "Product_Category", barmode="group", title = "Profit Earned by Product Category and Location", labels = {"Total_Profit": "Profit ($)", "Product_Category":"Product Category", "Store_Location": "Store Location"})
    return


@app.cell
def _(mo):
    mo.md(r"""The same pattern of overall profits grouped by category remains in both downtown and residential Locations. However, in commercial and airport locations, electronics jump above toys in earned profit and in airport locations alone, games jump above arts & crafts in earned profit. """)
    return


if __name__ == "__main__":
    app.run()
