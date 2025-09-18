import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Winter Storm Uri -- Demand Analysis

    In this notebook, we're going to analyze the impact of Winter Storm Uri on the electricity demand in Texas. We'll be using ERA5 temperature data along with a simple model for _Heating Degrees_.

    ## Approach

    - Calculate Heating Degrees using ERA5
    - Compute the long term daily climatology of heating degrees
    - Compare the actual heating degrees to the climatology
    """
    )
    return


@app.cell
def _():
    import xarray as xr
    from arraylake import Client
    from dask.diagnostics import ProgressBar
    import marimo as mo

    from energy import CLIMATE_EPOCH, TEXAS_METROS, TEXAS_BBOX, calculate_heating_degree
    from plot import plot_climatology
    return (
        CLIMATE_EPOCH,
        Client,
        ProgressBar,
        TEXAS_BBOX,
        TEXAS_METROS,
        calculate_heating_degree,
        mo,
        plot_climatology,
        xr,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Open the ERA5 dataset with Xarray

    In the previous notebook, we covered the basics of using the Arraylake client along with Icechunk and Xarray. So we'll mostly skip over that here.
    """
    )
    return


@app.cell
def _(Client):
    client = Client()
    return (client,)


@app.cell
def _(client):
    repo = client.get_repo("earthmover-public/era5-surface-aws")
    return (repo,)


@app.cell
def _(repo, xr):
    session = repo.readonly_session("main")

    era5 = xr.open_zarr(session.store, group="temporal", chunks={})
    era5
    return (era5,)


@app.cell
def _(TEXAS_BBOX, era5):
    texas_temp_hourly = (
        era5["t2"].sel(**TEXAS_BBOX) - 273.15
    )  # Convert from K to C

    texas_temp_hourly
    return (texas_temp_hourly,)


@app.cell(hide_code=True)
def _(calculate_heating_degree, mo):
    mo.md(
        f"""
    ## Heating Degrees Calculation

    Our model for Heating Degrees is quite simple -- calculating the number of degree-hours per day that the temperature is below a specified threshold (e.g. 18℃ or 64℉). The documentation for our model is below:

    ```
    {calculate_heating_degree.__doc__}
    ```

    ----------

    Below is the repr for the Xarray DataArray for heating degrees for every ERA5 pixel in Texas -- from 1975-2024!
    """
    )
    return


@app.cell
def _(calculate_heating_degree, texas_temp_hourly):
    hdd = calculate_heating_degree(texas_temp_hourly, aggregation="daily")
    hdd
    return (hdd,)


@app.cell(hide_code=True)
def _(CLIMATE_EPOCH, mo):
    mo.md(
        rf"""
    ## Calculate the climatology

    We want to compare the daily climatology to the actual heating degrees experienced during the winter storm. This is quick and easy using Xarray!

    Details:

    - For each day of the year, we'll calculate the mean and standard deviation
    - We'll use `CLIMATE_EPOCH={CLIMATE_EPOCH}`
    - Behind the scenes, `dask` will be providing parallel out-of-core computation
    """
    )
    return


@app.cell
def _(CLIMATE_EPOCH, ProgressBar, hdd):
    with ProgressBar():
        hdd_daily_temp = hdd.sel(time=CLIMATE_EPOCH).load()
    hdd_climatology_mean = hdd_daily_temp.groupby("time.dayofyear").mean(dim="time")
    hdd_climatology_std = hdd_daily_temp.groupby("time.dayofyear").std(dim="time")

    hdd_climatology_mean
    return hdd_climatology_mean, hdd_climatology_std


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Visualization

    Now that we've calculated the climatology, we're ready to build a visualization. Just like in the first notebook, we'll use some light Mariomo UI to create an interactive plot. 

    Notice how for most locations in Texas, this was a >2σ event!
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
    smooth_factor = mo.ui.number(start=1, stop=14, value=4, label="Rolling Window")
    return dropdown_dict, smooth_factor


@app.cell
def _(
    dropdown_dict,
    hdd,
    hdd_climatology_mean,
    hdd_climatology_std,
    mo,
    smooth_factor,
):
    # extract the lat/lon for the choosen location
    lat = dropdown_dict.value['latitude']
    lon = dropdown_dict.value['longitude']


    # Extract data for the choosen location
    climate_mean = (
        hdd_climatology_mean.sel(
            latitude=lat, longitude=lon, method="nearest"
        )
        .rolling(dayofyear=smooth_factor.value)
        .mean()
    )
    climate_std = (
        hdd_climatology_std.sel(
            latitude=lat, longitude=lon, method="nearest"
        )
        .rolling(dayofyear=smooth_factor.value)
        .mean()
    )

    actual = (
        hdd.sel(latitude=lat, longitude=lon, method="nearest")
        .sel(time=slice("2021-01-01", "2021-12-31"))
        .load()
    )
    actual.coords["dayofyear"] = actual["time.dayofyear"]
    actual_smooth = actual.rolling(time=smooth_factor.value).mean()

    mo.vstack([dropdown_dict, smooth_factor])
    return actual_smooth, climate_mean, climate_std, lat, lon


@app.cell
def _(
    actual_smooth,
    climate_mean,
    climate_std,
    dropdown_dict,
    plot_climatology,
):
    plot_climatology(
        smooth_mean=climate_mean,
        smooth_std=climate_std,
        smooth_actual=actual_smooth,
        title=f"HD Climatology for {dropdown_dict.selected_key}",
        ylabel="Heating Degree Hours (degree-hours/day)",
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Hourly Analysis

    We can also look at the hourly heating degree timeseries which clearly shows just how anomolous the demand on the energy system was.
    """
    )
    return


@app.cell
def _(calculate_heating_degree, lat, lon, texas_temp_hourly):
    hd_hourly = calculate_heating_degree(texas_temp_hourly, aggregation="hourly")

    hd_hourly.sel(
        latitude=lat, longitude=lon, method="nearest"
    ).sel(
        time=slice('2021-01-15', '2021-03-07')
    ).plot(c='k')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Conclusions and Next Steps

    In this notebook, we continued using our ERA5 dataset, this time analyzing the modeled heating degree load. We noticed that the event was a >2σ in basic every location we looked at.

    In the next notebook, we'll take our analysis one step further, analyzing the [production](./?file=production.py) side of the electricity equation.
    """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
