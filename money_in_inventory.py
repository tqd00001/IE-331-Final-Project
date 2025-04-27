

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
        #Question 4
        ##How much money is tied up in inventory at the toy stores? How long will it last?
        """
    )
    return


@app.cell
def _(pl):
    products = pl.read_parquet("pipeline/products.parquet")
    inventory = pl.read_parquet("pipeline/inventory.parquet")
    sales = pl.read_parquet("pipeline/sales.parquet")
    return inventory, products, sales


@app.cell
def _(inventory, pl, products):
    products_in_inventory = (
        inventory
        .group_by("Product_ID")
        .agg(
            pl.col("Stock_On_Hand").sum().alias("Stock")
        )
    )
    product_info = (
        products
        .select(["Product_ID", "Product_Name", "Product_Price"])
        .unique(subset=["Product_ID"])  
    )
    return product_info, products_in_inventory


@app.cell
def _(pl, product_info, products_in_inventory):
    tied_up = products_in_inventory.join(product_info, on="Product_ID")

    tied_up = tied_up.with_columns(
        (pl.col("Stock") * pl.col("Product_Price")).alias("Money_in_Inventory")
    ) 
    tied_up = tied_up.sort("Product_Name")
    return (tied_up,)


@app.cell
def _(px, tied_up):
    fig1 = px.bar(
        tied_up,
        x="Product_Name",
        y="Money_in_Inventory",
        barmode="group",
        title="Money Tied Up in Inventory by Product",
        labels={"Money_in_Inventory": "Inventory Value", "Product_Name": "Product Name"}
    )
    fig1
    return


@app.cell
def _(pl, tied_up):
    total_money_in_inventory = tied_up.select(
        pl.col("Money_in_Inventory").sum().alias("Total_Inventory_Value")
    )
    total_money_in_inventory
    return


@app.cell
def _(mo):
    mo.md(r"""The top products that have the most money tied up in inventory are Lego Bricks with $44.7K, Dinosaur Figures and Magic Sand being over $30K, and Rubik's Cubes with $29.9K. The total amount of money tied up in inventory is $410.2K.""")
    return


@app.cell
def _(pl, sales, tied_up):
    sale = sales.with_columns(
        pl.col("Date")
    )

    sales_span_days = (
        sale.select([
            pl.col("Date").min().alias("start"),
            pl.col("Date").max().alias("end")
        ])
        .with_columns([
            (pl.col("end") - pl.col("start")).dt.total_days().alias("days")
        ])
    )

    num_days = sales_span_days.select("days").item()

    total_sales_per_product = (
        sale
        .group_by("Product_ID")
        .agg(pl.col("Units").sum().alias("Total_Sold"))
    )

    avg_daily_sales = total_sales_per_product.with_columns(
        (pl.col("Total_Sold") / num_days).alias("Avg_Daily_Sales")
    )

    stock_and_sales = tied_up.join(avg_daily_sales, on="Product_ID", how="left")

    stock_and_sales = stock_and_sales.with_columns(
        (pl.col("Stock") / pl.col("Avg_Daily_Sales")).alias("Days_Left")
    )
    return (stock_and_sales,)


@app.cell
def _(px, stock_and_sales):
    fig2 = px.bar(
        stock_and_sales,
        x="Product_Name",
        y="Days_Left",
        title="Days Left in Inventory by Product",
        labels={"Days_Left": "Days Left", "Product_Name": "Product"},
        color="Days_Left",
    )
    fig2
    return


@app.cell
def _(mo):
    mo.md(r"""To address the question of how long money will stay tied up in inventory, estimated days left in inventory was plotted against product. After comparing both visuals, it appears that, generally, products that have the most amount of money tied up in inventory have the shortest amount of days left in inventory. This could be due to these products being high-value, and selling fast. Even though a lot of money is tied up, you’re likely to recoup that investment soon. Due to the high selling rate, stores want to have plenty in stock.""")
    return


if __name__ == "__main__":
    app.run()
