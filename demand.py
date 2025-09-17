import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import xarray as xr
    from arraylake import Client
    from dask.diagnostics import ProgressBar

    from energy import calculate_heating_degree_days
    from plot import plot_climatology

    TEXAS = dict(latitude=slice(37, 25), longitude=slice(253, 267))
    return (
        Client,
        ProgressBar,
        TEXAS,
        calculate_heating_degree_days,
        plot_climatology,
        xr,
    )


@app.cell
def _(mo):
    mo.md(
        r"""
    TODO's here:

    1. expand region to all of Texas
    2. evaluate hourly-hdd logic and make sure the units are right (we may want to /24)
    4. save the daily climatology to arraylake
    5. make a map that shows the impact of the cold event
    """
    )
    return


@app.cell
def _(Client):
    client = Client()
    client.login()
    client.list_repos("earthmover-public")
    return (client,)


@app.cell
def _(client):
    repo = client.get_repo("earthmover-public/era5-surface-aws")
    repo
    return (repo,)


@app.cell
def _(repo, xr):
    session = repo.readonly_session("main")

    era5 = xr.open_zarr(session.store, group="temporal")
    era5
    return (era5,)


@app.cell
def _(TEXAS, era5):
    texas_temp_hourly = (
        era5["t2"].sel(time=slice("1990-01-01", "2021-12-31"), **TEXAS) - 273.15
    )  # Convert from K to C

    # temporary bounding box for Austin Texas
    # texas_temp_hourly = texas_temp_hourly.sel(longitude=slice(260, 264), latitude=slice(33, 29))

    texas_temp_hourly
    return (texas_temp_hourly,)


@app.cell
def _(calculate_heating_degree_days, texas_temp_hourly):
    hdd = calculate_heating_degree_days(texas_temp_hourly, aggregation="daily")
    hdd
    return (hdd,)


@app.cell
def _(ProgressBar, hdd):
    with ProgressBar():
        temp1 = hdd.sel(time=slice("1990-01-01", "2019-12-31")).load()
    hdd_climatology_mean = temp1.groupby("time.dayofyear").mean(dim="time")
    hdd_climatology_std = temp1.groupby("time.dayofyear").std(dim="time")
    return hdd_climatology_mean, hdd_climatology_std


@app.cell
def _(hdd, hdd_climatology_mean, hdd_climatology_std):
    AUSTIN_LAT = 30.2672
    AUSTIN_LON = 97.7431

    smooth_factor = 7

    # Extract data for Austin location
    austin_mean = (
        hdd_climatology_mean.sel(
            latitude=AUSTIN_LAT, longitude=AUSTIN_LON, method="nearest"
        )
        .rolling(dayofyear=smooth_factor)
        .mean()
    )
    austin_std = (
        hdd_climatology_std.sel(
            latitude=AUSTIN_LAT, longitude=AUSTIN_LON, method="nearest"
        )
        .rolling(dayofyear=smooth_factor)
        .mean()
    )

    austin_actual = (
        hdd.sel(latitude=AUSTIN_LAT, longitude=AUSTIN_LON, method="nearest")
        .sel(time=slice("2021-01-01", "2021-12-31"))
        .load()
    )

    austin_actual.coords["dayofyear"] = austin_actual["time.dayofyear"]
    austin_actual_smooth = austin_actual.rolling(time=smooth_factor).mean()
    return (
        AUSTIN_LAT,
        AUSTIN_LON,
        austin_actual_smooth,
        austin_mean,
        austin_std,
    )


@app.cell
def _(
    AUSTIN_LAT,
    AUSTIN_LON,
    austin_actual_smooth,
    austin_mean,
    austin_std,
    plot_climatology,
):
    plot_climatology(
        smooth_mean=austin_mean,
        smooth_std=austin_std,
        smooth_actual=austin_actual_smooth,
        title=f"HDD Climatology for Austin, TX ({AUSTIN_LAT}°N, {360-AUSTIN_LON}°W)",
        ylabel="Heating Degree Hours (degree-hours/day)",
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
