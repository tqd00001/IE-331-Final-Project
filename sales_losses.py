

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.express as px
    return pl, px


@app.cell
def _(pl):
    products = pl.read_parquet("pipeline/products.parquet")
    return (products,)


@app.cell
def _(pl):
    inventory = pl.read_parquet("pipeline/inventory.parquet")

    out_of_stock = inventory.filter(pl.col("Stock_On_Hand") == 0)
    out_of_stock
    return (out_of_stock,)


@app.cell
def _(out_of_stock, pl):
    sales = pl.read_parquet("pipeline/sales.parquet")
    sales = sales.with_columns(
        pl.col("Date").cast(pl.Date)
    )
    OOS_and_sales = sales.join(out_of_stock, on=["Store_ID", "Product_ID"], how="inner")
    lost_sales = (
        OOS_and_sales
        .group_by(["Store_ID", "Product_ID"])
        .agg([
            pl.sum("Units").alias("Units_Lost")
        ])
    )
    lost_sales = lost_sales.sort("Store_ID")
    OOS_and_sales, lost_sales
    return (lost_sales,)


@app.cell
def _(pl):
    stores = pl.read_parquet("pipeline/stores.parquet")
    stores = stores.with_columns(
        pl.col("Store_Open_Date").cast(pl.Date)
    )
    return (stores,)


@app.cell
def _(lost_sales, products, stores):
    lost_sales_at_stores = (
        lost_sales
        .join(stores, on="Store_ID")
        .join(products, on="Product_ID")
        .select(["Store_ID", "Product_ID", "Units_Lost", "Store_Location","Store_Name", "Product_Name"])
    )
    lost_sales_at_stores
    return (lost_sales_at_stores,)


@app.cell
def _(lost_sales_at_stores, px):
    location_lost_sales_location = px.bar(
        lost_sales_at_stores,
        x="Store_Location",
        y="Units_Lost",
        color="Product_Name",
        title="Lost Sales due to Out of Stock Products by Store Location",
        labels = {"Units_Lost": "Units Lost", "Store_Location": "Store Location", "Product_Name": "Product Name"}
    )
    location_lost_sales_location
    return


@app.cell
def _(lost_sales_at_stores, px):
    location_lost_sales_name = px.bar(
        lost_sales_at_stores,
        x="Store_Name",
        y="Units_Lost",
        color="Product_Name",
        title="Lost Sales due to Out of Stock Products by Store",
        labels = {"Units_Lost": "Units Lost", "Store_Name": "Store Name", "Product_Name": "Product Name"}
    )
    location_lost_sales_name
    return


if __name__ == "__main__":
    app.run()
