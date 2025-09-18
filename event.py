import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Winter Storm Uri

    _Dates: February 13–17, 2021_

    This notebook explores the meteorological conditions that led to widespread power outages across Texas as a result of Winter Store Uri. 

    ## Approach

    We will use ECMWF's ERA5 Reanalysis to analyze the temperature and snow cover conditions leading up to, during, and after the storm.
    """
    )
    return


@app.cell
def _():
    from datetime import datetime

    import marimo as mo
    import matplotlib.pyplot as plt
    import xarray as xr
    from arraylake import Client

    from energy import TEXAS_BBOX, TEXAS_METROS
    from plot import plot_map

    return Client, TEXAS_BBOX, TEXAS_METROS, datetime, mo, plot_map, plt, xr


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Finding ERA5

    Earthmover's data catalog makes it easy to discover datasets. User can search for datasets in the Arraylake web app ([https://app.earthmover.io/](https://app.earthmover.io/)) or using the Python client interface. Below, we see the catalog for the `earthmover-public` organization.
    """
    )
    return


@app.cell
def _(Client):
    client = Client()

    # authenticate with Arraylake
    client.login()

    # list all repos in the earthmover-public organization
    client.list_repos("earthmover-public")
    return (client,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Accessing the ERA5 dataset

    After picking a dataset from the catalog, opening a dataset using Arraylake is as simple as calling `client.get_repo(REPO_NAME)`. Arraylake handles the access control, cloud credentials, and returns an Icechunk repository ready for use.
    """
    )
    return


@app.cell
def _(client):
    repo = client.get_repo("earthmover-public/era5-surface-aws")
    repo
    return (repo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## Opening ERA5 as an Xarray Dataset

    Now that we have an Icechunk repository we can easily open it using Xarray. We first open a "readonly session" pointing at the "main" branch. Then we ask Xarray to open the group using Icechunk's Zarr Store interface.
    """
    )
    return


@app.cell
def _(repo, xr):
    session = repo.readonly_session("main")

    era5 = xr.open_zarr(session.store, group="temporal")
    print(f"Size: {era5.nbytes / 1e12:.2f} TB")
    era5
    return (era5,)


@app.cell(hide_code=True)
def _(TEXAS_BBOX, mo):
    mo.md(
        f"""
    Despite its size, we can easily subset this dataset down to our region of interest. For this demo, that's the state of Texas.

    `TEXAS_BBOX={TEXAS_BBOX}`
    """
    )
    return


@app.cell
def _(TEXAS_BBOX, era5):
    texas_era5 = era5.sel(**TEXAS_BBOX)
    texas_era5
    return (texas_era5,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Analysis

    With our data in Xarray, we're now ready to start our analysis. First, let's look at a map of the average temperature during Winter Storm Uri (Feb 13-17). Then we'll also look at snow depth.
    """
    )
    return


@app.cell
def _(plot_map, texas_era5):
    temp_c = texas_era5.t2 - 273.15

    plot_map(
        temp_c.sel(time=slice("2021-02-13", "2021-02-17")).mean(dim="time"),
        title="Temperature",
        levels=14,
    )
    return


@app.cell
def _(plot_map, texas_era5):
    plot_map(
        texas_era5.sd.sel(time=slice("2021-02-13", "2021-02-17")).mean(dim="time"),
        title="Snow Depth",
        vmax=0.01,
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    What we can see from this is that most of Teaxas had a mean temperature below 0℃ and that substatial areas of the state were covered in snow.

    ### Time series at select metro areas

    Finally, we'll extract the hourly time series for temperature at the largest metro areas in Texas. We've created a simple interactive element using Marimo here but just below the surface is Xarray and Icechunk.
    """
    )
    return


@app.cell
def _(TEXAS_METROS, mo):
    dropdown_dict = mo.ui.dropdown(
        options=TEXAS_METROS,
        label="Metro Area:",
        value="Houston–The Woodlands–Sugar Land",
    )
    return (dropdown_dict,)


@app.cell
def _(dropdown_dict, mo):
    mo.vstack([dropdown_dict])
    return


@app.cell
def _(datetime, dropdown_dict, plt, texas_era5):
    fig1, ax1 = plt.subplots(figsize=(8, 4))

    (texas_era5.t2 - 273.15).sel(**dropdown_dict.value, method="nearest").sel(
        time=slice("2021-02-01", "2021-02-24")
    ).plot(c="#201F2C", ax=ax1)

    ax1.axvspan(
        datetime(2021, 2, 13),
        datetime(2021, 2, 17),
        color="#C396F9",
        alpha=0.3,
        label="axvspan",
    )
    ax1.axhline(0, color="#787878", linewidth=1, linestyle="--")

    ax1.set_ylabel("2m Air Temperature (C)")
    ax1.set_title(dropdown_dict.selected_key)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Conclusions and next steps

    In this notebook, we learned: 

    - How to use the Arraylake catalog to easily discover and access Icechunk repositories
    - How to open Icechunk repositories using Xarray
    - That Winter Storm Uri resulted in very cold temperatures across Texas.

    From here, we'll take this same data and analyze the impact on the electricity system. We'll start with [demand](./?file=demand.py), then move on to [production](./?file=production.py).
    """
    )
    return


if __name__ == "__main__":
    app.run()
