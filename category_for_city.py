

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
        #Question 5
        ##What is the most popular product category in each city and what city has the highest sales per category? 
        """
    )
    return


@app.cell
def _(pl):
    products = pl.read_parquet("pipeline/products.parquet")
    return (products,)


@app.cell
def _(pl):
    sales = pl.read_parquet("pipeline/sales.parquet")
    product_sales = (
        sales
        .group_by("Product_ID")
        .agg(
            pl.col("Units").sum().alias("Units_Sold_By_Product")
        )
        .sort("Product_ID")
        )
    return (sales,)


@app.cell
def _(pl):
    stores = pl.read_parquet("pipeline/stores.parquet")
    return (stores,)


@app.cell
def _(products, sales, stores):
    pop_cat_by_city = products.join(sales, on="Product_ID")
    pop_cat_by_city = pop_cat_by_city.join(stores, on="Store_ID")
    pop_cat_by_city = (
        pop_cat_by_city
        .select("Product_Category", "Store_City")
        .group_by("Product_Category","Store_City")
        .len()
        .sort("Product_Category", descending=True)
    )
    return (pop_cat_by_city,)


@app.cell
def _(pop_cat_by_city, px):
    px.bar(pop_cat_by_city, x = "Store_City", y = "len", color = "Product_Category", barmode="group", title = "Most Popular Product Category in Each City by Sales", labels = {"Store_City": "Store City", "Product_Category":"Product Category", "len": "Number of Sales per Category"})
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        In all cities except Morelia and Aguascalientes, toys or arts & crafts have the highest number of sales. In Morelia, games is the top category and while electronics are still being outsold by toys, they are outselling arts & crafts. In Aguascalientes, the games category is only outselling arts & crafts by two sales. 

        Most cities are sitting right around five thousand sales for their highest selling category, some spike into the ten to fifteen thousand range, but Cuidad de Mexico, Guadalajara, and Monterrey are outliers, with their top product categories selling nearly twenty to twenty five thousand products.

        This data is useful when deciding how to finance marketing. Some cities being larger than others introduces some proportionality of sales issues, but ultimately you would want to advertise to larger masses anyway. Additionally, this graph provides information on what you should advertise. You can push similar products to those you know people in the area have bought in the past, but you can also test out products that have not been so popular to see if they spark interest in a different product category. That woud have to be a more startegic decision. 
        """
    )
    return


if __name__ == "__main__":
    app.run()
