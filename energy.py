import numpy as np
import xarray as xr

TEXAS_METROS = {
    "Dallas–Fort Worth–Arlington": {"lat": 32.7767, "lon": 360 - 96.7970},  # 263.203
    "Houston–The Woodlands–Sugar Land": {
        "lat": 29.7604,
        "lon": 360 - 95.3698,
    },  # 264.630
    "San Antonio–New Braunfels": {"lat": 29.4241, "lon": 360 - 98.4936},  # 261.506
    "Austin–Round Rock–Georgetown": {"lat": 30.2672, "lon": 360 - 97.7431},  # 262.257
    "El Paso": {"lat": 31.7619, "lon": 360 - 106.4850},  # 253.515
    "McAllen–Edinburg–Mission": {"lat": 26.2034, "lon": 360 - 98.2300},  # 261.770
    "Corpus Christi": {"lat": 27.8006, "lon": 360 - 97.3964},  # 262.604
    "Brownsville–Harlingen": {"lat": 25.9017, "lon": 360 - 97.4975},  # 262.503
    "Laredo": {"lat": 27.5306, "lon": 360 - 99.4803},  # 260.520
    "Lubbock": {"lat": 33.5779, "lon": 360 - 101.8552},  # 258.145
}
TEXAS_BBOX = {"latitude": slice(37, 25), "longitude": slice(253, 267)}
CLIMATE_EPOCH = slice("1975-01-01", "2020-12-31")


def calculate_heating_degree_days(
    temperature_da: xr.DataArray, base_temp: float = 18.0, aggregation: str = "daily"
) -> xr.DataArray:
    """
    Calculate heating degree days from hourly xarray temperature data.

    Calculates HDD at hourly resolution first, then aggregates to desired period.
    This is the accurate method for energy trading applications.

    Parameters
    ----------
    temperature_da : xr.DataArray
        Hourly temperature DataArray in Celsius
    base_temp : float, default=18
        Base temperature in Celsius for HDD calculation (typically 18°C)
    aggregation : str, default='daily'
        How to aggregate hourly HDD ('daily', 'monthly', 'yearly', or 'hourly')

    Returns
    -------
    hdd : xr.DataArray
        Heating degree days DataArray aggregated to specified period
        Units are degree-hours for 'hourly', degree-hours/day for 'daily',
        degree-hours/month for 'monthly', degree-hours/year for 'yearly'
    """
    # Calculate hourly HDD: max(0, base_temp - temperature)
    hourly_hdd = xr.where(temperature_da < base_temp, base_temp - temperature_da, 0)

    # Set base attributes
    hourly_hdd.attrs = {
        "units": "degree-hours",
        "long_name": "Heating Degree Hours",
        "base_temperature": f"{base_temp}°C",
        "description": f"HDD calculated as max(0, {base_temp} - T) for each hour",
    }

    # Aggregate hourly HDD based on requested period
    if aggregation == "hourly":
        # Return hourly HDD without aggregation
        return hourly_hdd
    elif aggregation == "daily":
        # Sum hourly HDD to get daily total degree-hours
        hdd = hourly_hdd.resample(time="1D").sum()
        hdd.attrs.update(
            {
                "units": "degree-hours/day",
                "aggregation": "daily sum of hourly HDD",
                "long_name": "Daily Heating Degree Hours",
            }
        )
    elif aggregation == "monthly":
        # Sum hourly HDD to get monthly total degree-hours
        hdd = hourly_hdd.resample(time="1M").sum()
        hdd.attrs.update(
            {
                "units": "degree-hours/month",
                "aggregation": "monthly sum of hourly HDD",
                "long_name": "Monthly Heating Degree Hours",
            }
        )
    elif aggregation == "yearly":
        # Sum hourly HDD to get yearly total degree-hours
        hdd = hourly_hdd.resample(time="1Y").sum()
        hdd.attrs.update(
            {
                "units": "degree-hours/year",
                "aggregation": "yearly sum of hourly HDD",
                "long_name": "Yearly Heating Degree Hours",
            }
        )
    else:
        raise ValueError(
            f"Invalid aggregation: {aggregation}. Must be 'hourly', 'daily', 'monthly', or 'yearly'"
        )

    return hdd


def calculate_solar_production(
    ssrd: xr.DataArray,
    t2: xr.DataArray,
    panel_capacity_mw: xr.DataArray | float = 1.0,
    efficiency_ref: xr.DataArray | float = 0.20,
    temp_coeff: xr.DataArray = -0.004,
) -> xr.DataArray:
    """
    Calculate solar power production based on surface solar radiation and temperature.

    Parameters:
    - ssrd: Surface solar radiation downwards (J/m²) - cumulative over hour
    - t2: Temperature at 2m (K)
    - panel_capacity_mw: Installed solar capacity in MW
    - efficiency_ref: Reference efficiency at 25°C
    - temp_coeff: Temperature coefficient (typically -0.004/°C for silicon panels)

    Returns:
    - Solar power production in MW
    """
    # Convert cumulative radiation to average power (W/m²)
    # ssrd is accumulated over the hour, so divide by 3600 seconds
    irradiance = ssrd / 3600  # W/m²

    # Temperature correction (reference temperature is 25°C = 298.15K)
    temp_celsius = t2 - 273.15
    temp_correction = 1 + temp_coeff * (temp_celsius - 25)

    # Calculate production (assuming 1000 W/m² is the rated irradiance)
    production = (
        panel_capacity_mw * (irradiance / 1000) * efficiency_ref * temp_correction
    )

    # Clip negative values
    return production.clip(min=0)


def calculate_wind_speed(u: xr.DataArray, v: xr.DataArray) -> xr.DataArray:
    """Calculate wind speed from u and v components."""
    return np.sqrt(u**2 + v**2)


def calculate_wind_production(
    u100: xr.DataArray,
    v100: xr.DataArray,
    sp: xr.DataArray | None = None,
    t2: xr.DataArray | None = None,
    turbine_capacity_mw: xr.DataArray | float = 1.0,
) -> xr.DataArray:
    """
    Calculate wind power production based on wind speed at 100m height.

    Parameters:
    - u100, v100: Wind components at 100m height (m/s)
    - sp: Surface pressure (Pa) - optional for air density correction
    - t2: Temperature at 2m (K) - optional for air density correction
    - turbine_capacity_mw: Installed wind turbine capacity in MW

    Returns:
    - Wind power production in MW
    """
    # Calculate wind speed
    wind_speed = calculate_wind_speed(u100, v100)

    # Simple power curve approximation
    # Cut-in speed: 3 m/s
    # Rated speed: 12 m/s
    # Cut-out speed: 25 m/s

    production = xr.zeros_like(wind_speed)

    # Below cut-in
    production = xr.where(wind_speed < 3, 0, production)

    # Between cut-in and rated (cubic relationship)
    cubic_region = (wind_speed >= 3) & (wind_speed < 12)
    production = xr.where(
        cubic_region,
        turbine_capacity_mw * ((wind_speed - 3) / (12 - 3)) ** 3,
        production,
    )

    # Between rated and cut-out (constant at rated power)
    rated_region = (wind_speed >= 12) & (wind_speed < 25)
    production = xr.where(rated_region, turbine_capacity_mw, production)

    # Above cut-out
    production = xr.where(wind_speed >= 25, 0, production)

    # Air density correction if pressure and temperature are provided
    if sp is not None and t2 is not None:
        # Standard air density at sea level and 15°C
        rho_standard = 1.225  # kg/m³
        # Calculate actual air density using ideal gas law
        R_specific = 287.05  # J/(kg·K) for dry air
        rho_actual = sp / (R_specific * t2)
        # Apply density correction (power proportional to air density)
        density_correction = rho_actual / rho_standard
        production = production * density_correction

    return production


# Calculate hourly solar and wind production for the dataset
def calculate_renewable_production(ds: xr.Dataset) -> xr.Dataset:
    """
    Calculate both solar and wind production from ERA5 data.

    Parameters:
    - ds: xarray Dataset with ERA5 variables

    Returns:
    - Dataset with added solar_production and wind_production variables
    """
    # Solar production
    solar_prod = calculate_solar_production(ds["ssrd"], ds["t2"], panel_capacity_mw=1.0)

    # Wind production with air density correction
    wind_prod = calculate_wind_production(
        ds["u100"], ds["v100"], sp=ds["sp"], t2=ds["t2"], turbine_capacity_mw=2.0
    )

    # Add to dataset
    ds_out = xr.Dataset()
    ds_out["solar_production"] = solar_prod
    ds_out["wind_production"] = wind_prod

    # Add attributes
    ds_out["solar_production"].attrs = {
        "long_name": "Solar power production",
        "units": "MW",
        "description": "Estimated solar power production per MW installed capacity",
    }

    ds_out["wind_production"].attrs = {
        "long_name": "Wind power production",
        "units": "MW",
        "description": "Estimated wind power production per 2MW turbine",
    }

    return ds_out
