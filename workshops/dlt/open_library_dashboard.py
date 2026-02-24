"""Open Library Dashboard - marimo notebook.

Explore Harry Potter books data from the Open Library pipeline.
https://dlthub.com/docs/general-usage/dataset-access/marimo
"""

import marimo as mo

app = mo.App()


@app.cell
def __():
    import marimo as mo
    import plotly.express as px

    return mo, px


@app.cell
def __(mo):
    import dlt

    pipeline = dlt.attach(pipeline_name="open_library_pipeline")
    dataset = pipeline.dataset()
    return dataset, dlt, pipeline


@app.cell
def __(dataset):
    books_per_author_df = dataset("""
        SELECT value AS author, COUNT(DISTINCT _dlt_parent_id) AS book_count
        FROM search__author_name
        WHERE value IS NOT NULL
        GROUP BY value
        ORDER BY book_count DESC
        LIMIT 20
    """).df()
    return (books_per_author_df,)


@app.cell
def __(books_per_author_df, mo, px):
    bar_chart = px.bar(
        books_per_author_df,
        x="author",
        y="book_count",
        title="Number of Books per Author (Top 20)",
        labels={"author": "Author", "book_count": "Number of Books"},
    )
    bar_chart.update_layout(
        xaxis_tickangle=-45,
        xaxis={"categoryorder": "total descending"},
    )
    mo.vstack([
        mo.md("## Bar Chart: Books per Author"),
        mo.ui.plotly(bar_chart),
    ])
    return (bar_chart,)


@app.cell
def __(dataset):
    books_over_time_df = dataset("""
        SELECT first_publish_year AS year, COUNT(*) AS book_count
        FROM search
        WHERE first_publish_year IS NOT NULL
        GROUP BY first_publish_year
        ORDER BY first_publish_year
    """).df()
    return (books_over_time_df,)


@app.cell
def __(books_over_time_df, mo, px):
    line_chart = px.line(
        books_over_time_df,
        x="year",
        y="book_count",
        title="Books Published Over Time",
        labels={"year": "First Publish Year", "book_count": "Number of Books"},
    )
    line_chart.update_traces(mode="lines+markers")
    mo.vstack([
        mo.md("## Line Chart: Books Over Time"),
        mo.ui.plotly(line_chart),
    ])
    return (line_chart,)


if __name__ == "__main__":
    app.run()
