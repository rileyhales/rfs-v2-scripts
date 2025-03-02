import os
from glob import glob

import numcodecs
import xarray as xr
from natsort import natsorted

discharge_root = '/mnt/discharge'
configs_root = '/home/ubuntu/routing_configs'
zarr_root = '/mnt/zarr'
vpus = [d for d in natsorted(glob(os.path.join(discharge_root, '*'))) if os.path.isdir(d)]
resolution = 'daily'

if __name__ == '__main__':
    # make the first vpus's netcdfs into a zarr then append the others sequentially
    print('opening first vpu')
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
    print('writing zarr')
    (
        ds
        .chunk({'time': -1, 'river_id': 100})
        .to_zarr(
            '/mnt/zarr/final/daily.zarr',
            mode='w',
            zarr_format=2,
            compute=True,
            consolidated=True,
            encoding={
                'Q': {'compressor': numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.AUTOSHUFFLE)}
            },
        )
    )
    with open(f'/home/ubuntu/zarr-daily-complete', 'w') as f:
        f.write('complete')
