

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
    mo.md(
        r"""
        #Question 1
        ##Which product categories drive the biggest profits? Is this the same across store locations?
        """
    )
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
    return


@app.cell
def _(pl):
    sales = pl.read_csv("team-01/data/sales.csv")
    sales = (
        sales
        .group_by("Product_ID")
        .agg(
            pl.col("Units").sum().alias("Units_Sold_By_Product")
        )
        .sort("Product_ID")
        )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        It's creating a list because I used group_by().agg()
        Try with_cols first but actually think about if that makes sense before implementing
        """
    )
    return


app._unparsable_cell(
    r"""
    profit_by_category = products.join(sales, on=\"Product_ID\")
    profit_by_category = (
        profit_by_category
        .with_columns(
            pl.col(\"Product_Profit\")*pl.col(\"Units_Sold_By_Product\")).round(2))
            .alias(\"Total_Product_Profit\"
        )
        .group_by(\"Product_Category\")
        .agg(
            ((pl.col(\"Product_Profit\")*pl.col(\"Units_Sold_By_Product\")).round(2))
            .alias(\"Total_Product_Profit\")
        )
        .sort(\"Total_Product_Profit\")
    )
    profit_by_category
    """,
    name="_"
)


@app.cell
def _(profit_by_category, px):
    px.bar(profit_by_category, x = "Product_Category", y = "Total_Product_Profit")
    return


if __name__ == "__main__":
    app.run()
