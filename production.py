# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
# ]
# ///

import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Winter Storm Uri -- Production Analysis

    In this notebook, we're going to analyze the impact of Winter Storm Uri on the electricity production in Texas - focusing specifically on wind and solar production. We'll be using ERA5 meteorologic variables, EIA generation facility data, and simple production models.

    ## Approach

    - Extract hourly time series of meteorologic conditions at each solar and wind generation facility in Texas in February 2021
    - Feed these inputs into generation models for wind and solar
    - Compute the long term daily climatology of production
    - Compare the actual production to the climatology
    """
    )
    return


@app.cell
def _():
    import pandas as pd
    import xarray as xr
    from arraylake import Client
    from dask.diagnostics import ProgressBar
    import marimo as mo

    from energy import (
        CLIMATE_EPOCH,
        TEXAS_BBOX,
        TEXAS_METROS,
        calculate_renewable_production,
        calculate_solar_production,
        calculate_wind_production,
    )
    from plot import plot_climatology, plot_generator_map, plot_map
    return (
        CLIMATE_EPOCH,
        Client,
        ProgressBar,
        TEXAS_BBOX,
        calculate_renewable_production,
        calculate_solar_production,
        calculate_wind_production,
        mo,
        pd,
        plot_climatology,
        plot_generator_map,
        plot_map,
        xr,
    )


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Open the ERA5 dataset with Xarray

    In the previous notebook, we covered the basics of using the Arraylake client along with Icechunk and Xarray. So we'll mostly skip over that here. 

    It is worth noting however that we'll be using more than just temperature in this notebook. Specifically, we'll be using: 

    - `t2`: 2 metre temperature
    - `sp`: Surface pressure
    - `ssrd`: Surface solar radiation downwards
    - `u100`: 100 metre U wind component
    - `v100`: 100 metre V wind component
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

    era5 = xr.open_zarr(session.store, group="temporal")
    era5
    return (era5,)


@app.cell(hide_code=True)
def _(calculate_solar_production, calculate_wind_production, mo):
    mo.md(
        rf"""
    ## Calculate Renewable Power Production

    We have developed two simple models for calculating power production.

    #### Solar Model

    Our solar power production model is a function of the downward shortwave radiation at the surface and includes corrections for temperature and efficiency. 

    ```
    calculate_solar_production
    {calculate_solar_production.__doc__}
    ```

    #### Wind Model

    Our wind power production model is a function of the wind speed at 100m and includes corrections for air density (a function of surface pressure and temperature).

    ```
    calculate_wind_production
    {calculate_wind_production.__doc__}
    ```

    The models can be applied element wise, allowing us to explore power production in areas without current deployments.
    """
    )
    return


@app.cell
def _(calculate_renewable_production, era5):
    power_hour = calculate_renewable_production(era5)
    power_hour
    return (power_hour,)


@app.cell
def _(ProgressBar, TEXAS_BBOX, power_hour):
    with ProgressBar():
        power_map = power_hour.wind_production.sel(time='2024', **TEXAS_BBOX).mean(dim='time').load()
    return (power_map,)


@app.cell
def _(plot_map, power_map):
    plot_map(power_map)
    return


@app.cell
def _(mo):
    mo.md(r"""Conversely, we can easily extract a time series oof hourly power production from any point. For example,""")
    return


@app.cell
def _(power_hour):
    lat = 32.2
    lon = 258.6

    power_hour["solar_production"].sel(
        latitude=lat, longitude=lon, method="nearest"
    ).sel(time=slice("2021-01-15", "2021-02-28")).resample(time="1D").mean().plot()
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## EIA Generator Data

    To accurately predict the power production, we'll need to know the location and capacity of all active solar and wind generation facilities in the ERCOT system. For this, we'll use the January 2021 generator report from EIA-860M. This includes the list of active generators in the month before Winter Storm Uri, their location, and their nameplate capacity.
    """
    )
    return


@app.cell
def _(pd):
    all_generators = pd.read_excel("./data/january_generator2021.xlsx", header=2)
    return (all_generators,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Extract active solar facilities""")
    return


@app.cell
def _(all_generators):
    solar_generators = all_generators[
        (all_generators["Plant State"] == "TX")
        & (all_generators["Technology"] == "Solar Photovoltaic")
        & (all_generators["Operating Year"] <= 2021)
    ][
        [
            "Plant Name",
            "Entity ID",
            "Nameplate Capacity (MW)",
            "Latitude",
            "Longitude",
            "Operating Year",
            "Operating Month",
        ]
    ]
    active_solar_generators = solar_generators[
        (solar_generators["Operating Year"] < 2021)
        | (
            (solar_generators["Operating Year"] == 2021)
            & (solar_generators["Operating Month"] <= 2)
        )
    ]

    active_solar_generators
    return (active_solar_generators,)


@app.cell
def _(active_solar_generators, plot_generator_map):
    plot_generator_map(active_solar_generators, title="2021 Solar Generators")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Extract active wind facilities""")
    return


@app.cell
def _(all_generators):
    wind_generators = all_generators[
        (all_generators["Plant State"] == "TX")
        & (all_generators["Technology"].str.contains("Wind", case=False, na=False))
        & (all_generators["Operating Year"] <= 2021)
    ][
        [
            "Plant Name",
            "Entity ID",
            "Nameplate Capacity (MW)",
            "Latitude",
            "Longitude",
            "Operating Year",
            "Operating Month",
        ]
    ]

    active_wind_generators = wind_generators[
        (wind_generators["Operating Year"] < 2021)
        | (
            (wind_generators["Operating Year"] == 2021)
            & (wind_generators["Operating Month"] <= 2)
        )
    ]

    active_wind_generators
    return (active_wind_generators,)


@app.cell
def _(active_wind_generators, plot_generator_map):
    plot_generator_map(active_wind_generators, title="2021 Wind Generators")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Extract ERA5 data for all generator sites

    We need to join the EIA generator table with the ERA5 Xarray dataset. We do that here using Xarray's vectorized indexing:
    """
    )
    return


@app.cell
def _(active_wind_generators, era5):
    wind_sites_ds = active_wind_generators.to_xarray().rename({"index": "site"})
    era5_by_wind_sites = era5.sel(
        longitude=wind_sites_ds.Longitude,
        latitude=wind_sites_ds.Latitude,
        method="nearest",
    )
    era5_by_wind_sites["capacity"] = wind_sites_ds["Nameplate Capacity (MW)"]
    era5_by_wind_sites
    return (era5_by_wind_sites,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Calculate the per-site wind production

    Now that we have the ERA5 data extracted per site, we can run the production models again, this time scaling the outputs based on the nameplace capacity of each site.

    First we calculate the hourly production at each site:
    """
    )
    return


@app.cell
def _(calculate_wind_production, era5_by_wind_sites):
    wind_production = calculate_wind_production(
        era5_by_wind_sites["u100"],
        era5_by_wind_sites["v100"],
        sp=era5_by_wind_sites["sp"],
        t2=era5_by_wind_sites["t2"],
        turbine_capacity_mw=era5_by_wind_sites["capacity"],
    ).load()
    wind_production
    return (wind_production,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Then we calculate the climatology for total production across all sites:""")
    return


@app.cell
def _(CLIMATE_EPOCH, wind_production):
    # sum across sites and resample to daily frequency
    wind_production_daily = wind_production.sum("site").resample(time="1d").mean()

    # calculate the climatology
    wind_climatology_mean = (
        wind_production_daily.sel(time=CLIMATE_EPOCH).groupby("time.dayofyear").mean(dim="time")
    )
    wind_climatology_std = (
        wind_production_daily.sel(time=CLIMATE_EPOCH).groupby("time.dayofyear").std(dim="time")
    )
    return wind_climatology_mean, wind_climatology_std, wind_production_daily


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Wind Visualization 

    Now we can compare the actual wind re-forecast to the climatology.
    """
    )
    return


@app.cell
def _(
    plot_climatology,
    wind_climatology_mean,
    wind_climatology_std,
    wind_production_daily,
):
    smooth_factor = 4

    smooth_wind_mean = wind_climatology_mean.rolling(
        dayofyear=smooth_factor
    ).mean()
    smooth_wind_std = wind_climatology_std.rolling(
        dayofyear=smooth_factor
    ).mean()

    smooth_wind_actual = (
        wind_production_daily.rolling(time=smooth_factor)
        .mean()
        .sel(time=slice("2021-01-01", "2021-12-31"))
    )
    smooth_wind_actual.coords["dayofyear"] = smooth_wind_actual["time.dayofyear"]

    plot_climatology(
        smooth_mean=smooth_wind_mean,
        smooth_std=smooth_wind_std,
        smooth_actual=smooth_wind_actual,
        title="ERCOT Wind Production",
        ylabel="Wind Production (MW)",
    )
    return (smooth_factor,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Repeat for Solar production...""")
    return


@app.cell
def _(active_solar_generators, calculate_solar_production, era5):
    solar_sites_ds = active_solar_generators.to_xarray().rename({"index": "site"})
    era5_by_solar_sites = era5.sel(
        longitude=solar_sites_ds.Longitude,
        latitude=solar_sites_ds.Latitude,
        method="nearest",
    )
    era5_by_solar_sites["capacity"] = solar_sites_ds["Nameplate Capacity (MW)"]

    solar_production = calculate_solar_production(
        era5_by_solar_sites["ssrd"],
        era5_by_solar_sites["t2"],
        panel_capacity_mw=era5_by_solar_sites["capacity"],
        efficiency_ref=0.20,
        temp_coeff=-0.004,
    ).load()
    solar_production
    return (solar_production,)


@app.cell
def _(CLIMATE_EPOCH, solar_production):
    solar_production_daily = solar_production.sum("site").resample(time="1d").mean()

    solar_climatology_mean = (
        solar_production_daily.sel(time=CLIMATE_EPOCH)
        .groupby("time.dayofyear")
        .mean(dim="time")
    )
    solar_climatology_std = (
        solar_production_daily.sel(time=CLIMATE_EPOCH).groupby("time.dayofyear").std(dim="time")
    )
    return (
        solar_climatology_mean,
        solar_climatology_std,
        solar_production_daily,
    )


@app.cell
def _(
    plot_climatology,
    smooth_factor,
    solar_climatology_mean,
    solar_climatology_std,
    solar_production_daily,
):
    smooth_solar_mean = solar_climatology_mean.rolling(
        dayofyear=smooth_factor,
    ).mean()
    smooth_solar_std = solar_climatology_std.rolling(dayofyear=smooth_factor).mean()
    smooth_solar_actual = (
        solar_production_daily.rolling(time=smooth_factor)
        .mean()
        .sel(time=slice("2021-01-01", "2021-12-31"))
    )
    smooth_solar_actual.coords["dayofyear"] = smooth_solar_actual["time.dayofyear"]

    plot_climatology(
        smooth_mean=smooth_solar_mean,
        smooth_std=smooth_solar_std,
        smooth_actual=smooth_solar_actual,
        title="ERCOT Solar Production",
        ylabel="Solar Production (MW)",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Conclusions

    We have now analyzed the meteorologic, demand, and production sides of the Winter Storm Uri.

    Observations:

    - Temperatures and thus heating degree metrics were far outside the normal range for most of Texas
    - Renewables (wind and solar) were both much closer to "normal" ranges

    Along the way, we learned:

    - How to use the Arraylake catalog to discover and access datasets
    - How to use Icechunk and Xarray to quickly and efficiently access and process large gridded weather datasets like ERA5
    """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
