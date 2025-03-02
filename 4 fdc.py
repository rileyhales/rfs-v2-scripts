import os
from multiprocessing import Pool

import numpy as np
import xarray as xr

# Configuration parameters
zarr_variable = 'Q'
earliest_date = '1950-01-01'
latest_date = '2024-12-31'
resolution = 'daily'
n_rivers = xr.open_zarr(f'/mnt/zarr/final/{resolution}.zarr').river_id.shape[0]
base_step_size = 140
os.makedirs('/mnt/zarr/fdc', exist_ok=True)


def hourly_chunk_to_fdcs(start_index) -> None:
    """
    Convert hydrograph data to Flow Duration Curves (FDCs) for given river IDs.
    Saves results in zarr v2 format with specific coordinate structure.
    """
    step_size = base_step_size * 24 if resolution == 'hourly' else base_step_size
    with xr.open_zarr(f'/mnt/zarr/final/{resolution}.zarr') as ds:
        # Select the hydrographs for the river_ids
        df = (
            ds
            [zarr_variable]
            .isel(river_id=slice(start_index, min(start_index + step_size, n_rivers)))
            .to_dataframe()
            [zarr_variable]
            .reset_index()
            .pivot(columns='river_id', values=zarr_variable, index='time')
            .loc[earliest_date:latest_date]
        )

        # Define the percentiles in a gap of 1
        percentiles = np.arange(100, -1, -1)
        p_exceed = 100 - percentiles

        # Annual calculations
        annual_percentiles = np.percentile(df.values, percentiles, axis=0)

        # Create xarray DataArray for annual FDC
        annual_da = xr.DataArray(
            data=annual_percentiles,
            coords={
                'p_exceed': p_exceed,
                'river_id': df.columns
            },
            dims=['p_exceed', 'river_id'],
            name=f'fdc_{resolution}_total'
        )

        # Create xarray Dataset for annual FDC
        annual_ds = annual_da.to_dataset()

        # Save annual FDCs in zarr v2 format
        annual_file_name = os.path.join('/mnt/zarr/fdc', f'fdc_{resolution}_annual_{start_index}.zarr')
        encoding = {var: {'compressor': None} for var in annual_ds.data_vars}
        annual_ds.to_zarr(
            annual_file_name,
            mode='w',
            zarr_format=2,
            encoding=encoding,
            consolidated=True
        )

        # Monthly calculations
        monthly_data = []
        for month in range(1, 13):
            month_percentiles = np.percentile(df[df.index.month == month].values, percentiles, axis=0)
            monthly_data.append(month_percentiles)

        # Stack monthly data along new month dimension
        monthly_array = np.stack(monthly_data, axis=0)

        # Create xarray DataArray for monthly FDC
        monthly_da = xr.DataArray(
            data=monthly_array,
            coords={
                'month': np.arange(1, 13),
                'p_exceed': p_exceed,
                'river_id': df.columns
            },
            dims=['month', 'p_exceed', 'river_id'],
            name=f'fdc_{resolution}'
        )

        # Create xarray Dataset for monthly FDC
        monthly_ds = monthly_da.to_dataset()

        # Save monthly FDCs in zarr v2 format
        monthly_file_name = os.path.join('/mnt/zarr/fdc', f'fdc_{resolution}_monthly_{start_index}.zarr')
        encoding = {var: {'compressor': None} for var in monthly_ds.data_vars}
        monthly_ds.to_zarr(
            monthly_file_name,
            mode='w',
            zarr_format=2,
            encoding=encoding,
            consolidated=True
        )
    return


if __name__ == '__main__':
    step_size = base_step_size * 24 if resolution == 'hourly' else base_step_size
    jobs = list(range(0, n_rivers, step_size))

    # Process chunks in parallel with timeout
    with Pool(80) as p:
        for job in jobs:
            p.apply_async(hourly_chunk_to_fdcs, args=(job,))
        p.close()
        p.join()

import xarray as xr
from concurrent.futures import ThreadPoolExecutor as ThreadPool
import dask

if __name__ == '__main__':
    n = 60
    with dask.config.set(scheduler='threads', num_workers=n, pool=ThreadPool(n)):
        ds = (
            xr
            .load_dataset('/mnt/zarr/fdc_hourly.nc')
            .rename({
                'fdc_daily_annual': 'fdc_daily',
                'fdc_hourly_annual': 'fdc_hourly',
            })
        )
        ds.attrs = {
            "title": "River Forecast System v2 Monthly Retrospective Simulation",
            "description": "Flow duration curves based on either hourly or daily average simulations, either by month or based on all data.",
            "author": "Riley Chad Hales, PhD",
            "creation_date": "2025-01-21",
            "license": "CC-BY-SA 4.0",
            "copyright": "2025",
            "revision": "1",
        }
        (
            ds
            .chunk({
                'p_exceed': 101,
                'month': 12,
                'river_id': 1000,
            })
            .to_zarr(
                '/mnt/zarr/final/fdc.zarr',
                mode='w',
                zarr_format=2,
                consolidated=True,
                # encoding={
                #     var: {'compressor': {'id': 'zstd', 'level': 5}} for var in ds.data_vars
                # },
                compute=True,
            )
        )

# xr.open_zarr('/mnt/zarr/fdc_daily.zarr').to_netcdf('/mnt/fdc_daily.nc')
# xr.open_zarr('/mnt/zarr/fdc_hourly.zarr').to_netcdf('/mnt/fdc_hourly.nc')
