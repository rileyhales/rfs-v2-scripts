import os
from glob import glob

import numcodecs
import pandas as pd
import xarray as xr
from natsort import natsorted

discharge_root = '/mnt/discharge/MAXES'
configs_root = '/home/ubuntu/routing_configs'
vpus = [d for d in natsorted(glob(os.path.join(discharge_root, '*'))) if os.path.isdir(d)]

if __name__ == '__main__':
    # make the first vpus's netcdfs into a zarr then append the others sequentially
    print(f'opening first vpu {vpus[0]}')
    ds = xr.open_mfdataset(natsorted(glob(os.path.join(vpus[0], f'Q_monmax_*.nc'))))
    for vpu in vpus[1:]:
        print(f'concatenating with vpu {vpu}')
        ds = (
            xr.concat(
                [ds, xr.open_mfdataset(natsorted(glob(os.path.join(vpu, f'Q_monmax_*.nc'))))],
                dim='river_id',
            )
        )
    # get the maximum value by year by grouping by year and evaluating the maximum
    ds = ds.groupby('time.year').max(dim='time')
    ds.attrs = {
        "title": f"River Forecast System v2 Retrospective Simulation Annual Maximums",
        "description": f"Retrospective simulation of global rivers since 1940 based on TDX-Hydro hydrography, ERA5 meteorology reanalysis, and matrix Muskingum vector routing using river-route.",
        "author": "Riley Chad Hales, PhD",
        "creation_date": "2025-02-22",
        "license": "CC-BY-NC-SA 4.0",
        "copyright": "2025",
        "revision": "1"
    }
    year_datetimes = pd.to_datetime([f'{year}-01-01' for year in ds.year.values])
    print('writing zarr')
    (
        ds
        # rename the year dimension to time
        .rename({'year': 'time'})
        .assign_coords({'time': year_datetimes})
        .chunk({'time': -1, 'river_id': 1_000})
        .to_zarr(
            '/mnt/zarr/final/maximums.zarr',
            mode='w',
            zarr_format=2,
            compute=True,
            consolidated=True,
            encoding={
                'Q': {'compressor': numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.AUTOSHUFFLE)}
            },
        )
    )
    print('completed')
