from multiprocessing import Pool

import numpy as np
import pandas as pd
import requests
import datetime
import xarray as xr
from scipy.stats import pearson3, kurtosis
from scipy.stats import weibull_min

# Open dataset without Dask for direct loading
return_periods = np.array([2, 5, 10, 25, 50, 100])
# dataset = "/Users/rchales/data/maximums.zarr"
rechunked = "/Users/rchales/data/maximums_rechunked.zarr"


# dataset = '/home/ubuntu/maximums.zarr'
# rechunked = '/home/ubuntu/maximums_rechunked.zarr'


def compute_lp3_rp(series):
    values = np.log10(series + 1)
    params = pearson3.fit(values)
    rps = np.power(10, [pearson3.ppf(1 - 1 / yr, *params) for yr in return_periods])
    return pd.Series(rps, index=return_periods, name=series.name)


def compute_weibull_rp(series):
    params = weibull_min.fit(series)
    rps = [weibull_min.ppf(1 - 1 / yr, *params) for yr in return_periods]
    return pd.Series(rps, index=return_periods, name=series.name)


def compute_gumbel_rp(df):
    std = np.nanstd(df.values, axis=1)
    xbar = np.nanmean(df.values, axis=1)
    rps = np.array(
        [np.round(-np.log(-np.log(1 - (1 / rp))) * std * .7797 + xbar - (.45 * std), 2) for rp in return_periods])
    rps = pd.DataFrame(rps, index=return_periods, columns=df.index).round(3)
    (
        xr
        .Dataset(
            {'gumbel': (['return_period', 'river_id'], rps.values)},
            coords={'river_id': rps.columns, 'return_period': return_periods}
        )
        .to_netcdf('return_periods_gumbel.nc')
    )


def compute_logpearson3_rp(df):
    n = df.shape[1]
    logx = np.log10(df.values + 1)
    mean = np.nanmean(logx, axis=1).reshape(-1, 1)
    std = np.nanstd(logx, axis=1)
    skew = (n * np.power(logx - mean, 3).sum(axis=1)) / ((n - 1) * (n - 2) * np.power(std, 3))
    kt = np.array([pearson3.ppf(1 - 1 / yr, skew) for yr in return_periods]).T
    rps = np.power(10, mean + std.reshape(-1, 1) * kt).T
    rps = pd.DataFrame(rps, index=return_periods, columns=df.index).round(3)
    (
        xr
        .Dataset(
            {'logpearson3': (['return_period', 'river_id'], rps.values)},
            coords={'river_id': rps.columns, 'return_period': return_periods}
        )
        .to_netcdf('return_periods_lp3.nc')
    )


if __name__ == '__main__':
    ds = xr.open_zarr(rechunked)

    df = pd.DataFrame(
        ds['Q'].values.T,
        index=ds['river_id'].values,
        columns=ds['time'].values
    )
    jobs = range(df.shape[0])
    compute_gumbel_rp(df)
    compute_logpearson3_rp(df)
    ds1 = xr.open_dataset('return_periods_gumbel.nc')
    ds2 = xr.open_dataset('return_periods_lp3.nc')
    ds1.merge(ds2).to_zarr(
        'return-periods.zarr',
        mode='w', zarr_format=2, consolidated=True,
        encoding={
            'gumbel': {'chunks': (6, 50_000), 'compressor': None},
            'logpearson3': {'chunks': (6, 50_000), 'compressor': None}
        }
    )
