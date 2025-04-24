

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
    products = pl.read_parquet("pipeline/products.parquet")
    return (products,)


@app.cell
def _(pl):
    sales = pl.read_parquet("pipeline/sales.parquet")
    return (sales,)


@app.cell
def _(pl, products, sales):
    product_sales = products.join(sales, on="Product_ID")
    product_sales = (
        product_sales
        .with_columns(
            pl.col("Date").dt.month().alias("Month")
        )
    )
    product_sales = (
        product_sales
        .with_columns(
            pl.when(pl.col("Month").is_in([12, 1, 2]))
            .then(pl.lit("Winter"))
            .when(pl.col("Month").is_in([3, 4, 5]))
            .then(pl.lit("Spring"))
            .when(pl.col("Month").is_in([6, 7, 8]))
            .then(pl.lit("Summer"))
            .when(pl.col("Month").is_in([9, 10, 11]))
            .then(pl.lit("Fall"))
            .alias("Season")
        )
    )
    product_sales
    return


if __name__ == "__main__":
    app.run()
