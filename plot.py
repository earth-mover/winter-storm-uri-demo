import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr
from matplotlib.axes import Axes
from matplotlib.figure import Figure


def plot_climatology(
    smooth_mean: xr.DataArray,
    smooth_std: xr.DataArray,
    smooth_actual: xr.DataArray,
    title: str = "",
    ylabel: str = "",
) -> Figure:
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 6))
    # Plot the mean line
    smooth_mean.plot(ax=ax, color="#A653FF", linewidth=2, label="Mean HDD")

    # Add shaded region for ±1 standard deviation
    ax.fill_between(
        smooth_mean.dayofyear,
        (smooth_mean - smooth_std).clip(min=0),
        smooth_mean + smooth_std,
        alpha=0.3,
        color="#A653FF",
        label="±1 std dev",
    )

    # Optionally add ±2 standard deviation region for reference
    ax.fill_between(
        smooth_mean.dayofyear,
        (smooth_mean - 2 * smooth_std).clip(min=0),
        smooth_mean + 2 * smooth_std,
        alpha=0.15,
        color="#A653FF",
        label="±2 std dev",
    )

    # Optionally add ±3 standard deviation region for reference
    ax.fill_between(
        smooth_mean.dayofyear,
        (smooth_mean - 3 * smooth_std).clip(min=0),
        smooth_mean + 3 * smooth_std,
        alpha=0.08,
        color="#A653FF",
        label="±3 std dev",
    )

    smooth_actual.plot(x="dayofyear", ax=ax, label="2021", c="#201F2C")

    # Customize the plot
    ax.set_title(title, fontsize=14)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)

    # Add month labels on x-axis for reference
    month_starts = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
    month_labels = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    ax.set_xticks(month_starts)
    ax.set_xticklabels(month_labels)

    plt.tight_layout()
    return fig


def plot_generator_map(df: pd.DataFrame, title: str = "") -> Axes:
    # prepare plot
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.STATES)
    ax.set_xlim(253 - 360, 267 - 360)
    ax.set_ylim(25, 38)

    # set gridlines
    gl = ax.gridlines(draw_labels=True)
    gl.top_labels = False
    gl.right_labels = False

    # plot w/ color=capacity
    df.plot.scatter(
        x="Longitude",
        y="Latitude",
        c="Nameplate Capacity (MW)",
        colormap="plasma",
        ax=ax,
    )

    # set title
    if title:
        ax.set_title(title)

    return ax


def plot_map(da: xr.DataArray, title: str = "", **kwargs) -> Axes:
    p = da.plot(
        subplot_kws={"projection": ccrs.PlateCarree(), "facecolor": "gray"},
        transform=ccrs.PlateCarree(),
        cbar_kwargs={
            "orientation": "horizontal",
            "pad": 0.05,  # distance from plot
            "aspect": 50,  # length-to-height ratio
            "shrink": 0.6,  # shrink relative to default size
        },
        **kwargs,
    )
    p.axes.coastlines()
    p.axes.add_feature(cfeature.STATES)
    p.axes.set_title(title)

    return p.axes
