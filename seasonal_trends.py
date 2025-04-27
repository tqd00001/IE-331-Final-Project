

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
    sales = pl.read_parquet("pipeline/sales.parquet")
    sales
    return (sales,)


@app.cell
def _(pl, sales):
    sales_with_seasons = (
        sales.with_columns([
            pl.col("Date").dt.month().alias("Month")
        ])
    )
    sales_with_seasons = (
        sales_with_seasons.with_columns(
            pl.when(pl.col("Month").is_in([12, 1, 2])).then(pl.lit("Winter"))
                .when(pl.col("Month").is_in([3, 4, 5])).then(pl.lit("Spring"))
                .when(pl.col("Month").is_in([6, 7, 8])).then(pl.lit("Summer"))
                .when(pl.col("Month").is_in([9, 10, 11])).then(pl.lit("Fall"))
                .alias("Season")
        )
    )
    sales_with_seasons
    return (sales_with_seasons,)


@app.cell
def _(pl, sales_with_seasons):
    seasonal_sales = (
        sales_with_seasons
        .group_by("Season")
        .agg(pl.col("Units").sum().alias("Units_Sold"))
    )
    seasonal_sales
    return (seasonal_sales,)


@app.cell
def _(px, seasonal_sales):
    seasonal_trends_bar = px.bar(
        seasonal_sales,
        x="Season",
        y="Units_Sold",
        title="Total Units Sold by Season",
        color="Season",
        labels = {"Units_Sold": "Units Sold", "Season": "Season"}
    )
    seasonal_trends_bar
    return


if __name__ == "__main__":
    app.run()
