# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
# ]
# ///

import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import pandas as pd
    import xarray as xr
    from arraylake import Client

    from energy import (
        TEXAS_METROS,
        calculate_renewable_production,
        calculate_solar_production,
        calculate_wind_production,
    )
    from plot import plot_climatology, plot_generator_map

    return (
        Client,
        TEXAS_METROS,
        calculate_renewable_production,
        calculate_solar_production,
        calculate_wind_production,
        pd,
        plot_climatology,
        plot_generator_map,
        xr,
    )


@app.cell
def _(Client):
    client = Client()

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
def _(calculate_renewable_production, era5):
    power_hour = calculate_renewable_production(era5)
    power_hour
    return (power_hour,)


@app.cell
def _(TEXAS_METROS, power_hour):
    lat = TEXAS_METROS["Houston–The Woodlands–Sugar Land"]["lat"]
    lon = TEXAS_METROS["Houston–The Woodlands–Sugar Land"]["lon"]

    power_hour["solar_production"].sel(
        latitude=lat, longitude=lon, method="nearest"
    ).sel(time=slice("2021-01-15", "2021-02-28")).resample(time="1D").mean().plot()
    return


@app.cell
def _(pd):
    all_generators = pd.read_excel("./data/january_generator2021.xlsx", header=2)
    all_generators
    return (all_generators,)


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


@app.cell
def _(active_solar_generators, plot_generator_map):
    plot_generator_map(active_solar_generators, title="2021 Solar Generators")
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


@app.cell
def _(EPOCH, wind_production):
    wind_production_daily = wind_production.sum("site").resample(time="1d").mean()

    wind_climatology_mean = (
        wind_production_daily.sel(time=EPOCH).groupby("time.dayofyear").mean(dim="time")
    )
    wind_climatology_std = (
        wind_production_daily.sel(time=EPOCH).groupby("time.dayofyear").std(dim="time")
    )
    return wind_climatology_mean, wind_climatology_std, wind_production_daily


@app.cell
def _(
    plot_climatology,
    wind_climatology_mean,
    wind_climatology_std,
    wind_production_daily,
):
    smooth_factor = 7

    smooth_wind_mean = wind_climatology_mean.rolling(
        dayofyear=smooth_factor, center=True
    ).mean()
    smooth_wind_std = wind_climatology_std.rolling(
        dayofyear=smooth_factor, center=True
    ).mean()

    smooth_wind_actual = (
        wind_production_daily.rolling(time=smooth_factor, center=True)
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
def _(EPOCH, solar_production):
    solar_production_daily = solar_production.sum("site").resample(time="1d").mean()

    solar_climatology_mean = (
        solar_production_daily.sel(time=EPOCH)
        .groupby("time.dayofyear")
        .mean(dim="time")
    )
    solar_climatology_std = (
        solar_production_daily.sel(time=EPOCH).groupby("time.dayofyear").std(dim="time")
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


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
