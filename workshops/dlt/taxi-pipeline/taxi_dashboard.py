import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import dlt

    pipeline = dlt.attach(pipeline_name="taxi_pipeline")
    dataset = pipeline.dataset()
    return (dataset,)


@app.cell
def _(dataset, mo):
    # Access the ibis connection via the dataset
    df = dataset.table("trips").df()

    mo.ui.table(df)
    return (df,)


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""

        SELECT ((SELECT COUNT(*) 
        		FROM df
        		WHERE LOWER(payment_type) = 'credit') / 
            	COUNT(*)) * 100
        			AS percentage_of_credit_card_payments
        FROM df;

        """
    )
    return


@app.cell(hide_code=True)
def _(df, mo):
    _df = mo.sql(
        f"""
        SELECT SUM(tip_amt) 
        FROM df;
        """
    )
    return


if __name__ == "__main__":
    app.run()
