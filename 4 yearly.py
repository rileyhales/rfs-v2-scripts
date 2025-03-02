import numcodecs
import pandas as pd
import xarray as xr

if __name__ == '__main__':
    ds = (
        xr
        .open_zarr('/mnt/zarr/final/monthly-timeseries.zarr')
        .resample(time='YE')
        .mean()
    )
    times = ds['time'].values
    force_left_aligned_times = pd.to_datetime(times)
    force_left_aligned_times = [f'{year}-01-01' for year in force_left_aligned_times.year]
    force_left_aligned_times = pd.to_datetime(force_left_aligned_times)
    (
        ds
        .assign_coords({'time': force_left_aligned_times})
        .chunk({'time': -1, 'river_id': 1_000})
        .to_zarr(
            '/mnt/zarr/final/yearly-timeseries.zarr',
            mode='w',
            zarr_format=2,
            compute=True,
            consolidated=True,
            encoding={
                'Q': {'compressor': numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.AUTOSHUFFLE)}
            },
        )
    )
    # now read and rechunk that file to timesteps oriented
    (
        xr
        .open_zarr('/mnt/zarr/final/yearly-timeseries.zarr')
        .chunk({'time': 1, 'river_id': 2_500_000})
        .to_zarr(
            '/mnt/zarr/final/yearly-timesteps.zarr',
            mode='w',
            zarr_format=2,
            compute=True,
            consolidated=True,
            encoding={
                'Q': {'compressor': numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.AUTOSHUFFLE)}
            },
        )
    )
