import os
from glob import glob

import numcodecs
import pandas as pd
import xarray as xr
from natsort import natsorted

discharge_root = '/mnt/discharge'
configs_root = '/home/ubuntu/routing_configs'
vpus = [d for d in natsorted(glob(os.path.join(discharge_root, '*'))) if os.path.isdir(d)]
resolution = 'monthly'

if __name__ == '__main__':
    # make the first vpus's netcdfs into a zarr then append the others sequentially
    print(f'opening first vpu {vpus[0]}')
    ds = xr.open_mfdataset(natsorted(glob(os.path.join(vpus[0], f'Q_{resolution}_*.nc'))))
    for vpu in vpus[1:]:
        print(f'concatenating with vpu {vpu}')
        ds = (
            xr.concat([
                ds,
                xr.open_mfdataset(natsorted(glob(os.path.join(vpu, f'Q_{resolution}_*.nc'))))
            ], dim='river_id', )
        )
    ds.attrs = {
        "title": f"River Forecast System v2 {resolution.title()} Retrospective Simulation",
        "description": f"{resolution.title()} simulation of global rivers since 1940 based on TDX-Hydro hydrography, ERA5 meteorology reanalysis, and matrix Muskingum vector routing using river-route.",
        "author": "Riley Chad Hales, PhD",
        "creation_date": "2025-02-22",
        "license": "CC-BY-NC-SA 4.0",
        "copyright": "2025",
        "revision": "1"
    }
    times = pd.to_datetime(ds['time'].values)
    force_left_aligned_times = [f'{t.year}-{t.month:02d}-01' for t in times]
    force_left_aligned_times = pd.to_datetime(force_left_aligned_times)
    force_left_aligned_times = force_left_aligned_times + pd.DateOffset(months=1)
    print('writing zarr')
    (
        ds
        .assign_coords({'time': force_left_aligned_times})
        .chunk({'time': -1, 'river_id': 1_000})
        .to_zarr(
            '/mnt/zarr/final/monthly-timeseries.zarr',
            mode='w',
            zarr_format=2,
            compute=True,
            consolidated=True,
            encoding={
                'Q': {'compressor': numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.AUTOSHUFFLE)}
            },
        )
    )
    with open(f'/home/ubuntu/zarr-{resolution}-complete', 'w') as f:
        f.write('complete')

    print('rewriting zarr with timesteps oriented chunks')
    (
        xr
        .open_zarr('/mnt/zarr/final/monthly-timeseries.zarr')
        .chunk({'time': 1, 'river_id': 1_000_000})
        .to_zarr(
            '/mnt/zarr/final/monthly-timesteps.zarr',
            mode='w',
            zarr_format=2,
            compute=True,
            consolidated=True,
            encoding={
                'Q': {'compressor': numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.AUTOSHUFFLE)}
            },
        )
    )
    with open(f'/home/ubuntu/zarr-{resolution}-timesteps-complete', 'w') as f:
        f.write('complete')
