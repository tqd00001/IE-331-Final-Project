

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return (pl,)


@app.cell
def _(pl):
    inventory = pl.read_csv("team-01/data/inventory.csv")
    inventory.write_parquet("pipeline/inventory.parquet")
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
                 pl.col("Product_Name"),
                 pl.col("Product_Category"),
                 pl.col("Product_Cost").cast(pl.Float32),
                 pl.col("Product_Price").cast(pl.Float32)
             )
    )
    products.write_parquet("pipeline/products.parquet")
    return


@app.cell
def _(pl):
    sales = pl.read_csv("team-01/data/sales.csv")
    sales = sales.with_columns(
            pl.col("Date").cast(pl.Date)
        )
    sales.write_parquet("pipeline/sales.parquet")
    return


@app.cell
def _(pl):
    stores = pl.read_csv("team-01/data/stores.csv")
    stores = stores.with_columns(
            pl.col("Store_Open_Date").cast(pl.Date)
        )
    stores.write_parquet("pipeline/stores.parquet")
    return


if __name__ == "__main__":
    app.run()
