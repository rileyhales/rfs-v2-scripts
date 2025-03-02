import os
from concurrent.futures import ThreadPoolExecutor as ThreadPool
from glob import glob
from multiprocessing import Pool

import dask
import numcodecs
import xarray as xr
from natsort import natsorted

discharge_root = '/mnt/discharge'
zarr_root = '/mnt/zarr/hourly'
final_root = '/mnt/zarr/final'
configs_root = '/home/ubuntu/routing_configs'
os.makedirs(zarr_root, exist_ok=True)
os.makedirs(final_root, exist_ok=True)


def convert(vpu):
    try:
        n = 15
        with dask.config.set(scheduler='threads', num_workers=n, pool=ThreadPool(n)):
            print(f'working on {vpu}')
            output_zarr = f'{zarr_root}/{vpu}.zarr'
            netcdfs = list(natsorted(glob(f'{discharge_root}/{vpu}/Q_hourly*.nc')))

            # Skip processing if input files are missing or output exists
            if os.path.exists(f'/home/ubuntu/{vpu}_zarr_complete.txt'):
                print(f'------------Skipping {vpu}: zarr conversion already complete')
                return
            if os.path.exists(output_zarr):
                print(f'------------Error {output_zarr}: zarr already exists')
                return
            if len(netcdfs) != 1020:
                print(f'------------Error {output_zarr}: 1020 source netcdfs not found')
                return

            # Use Dask to process the dataset in parallel
            (
                xr
                .open_mfdataset(netcdfs, combine='by_coords', parallel=True)
                .chunk({'time': -1, 'river_id': 500})
                .to_zarr(
                    output_zarr,
                    mode='w',
                    zarr_format=2,
                    consolidated=True,
                    compute=True,
                    encoding={
                        'Q': {
                            'compressor': numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.AUTOSHUFFLE)
                        }
                    }
                )
            )
            print(f'\tFinished zarr conversion for {vpu}')
            with open(f'/home/ubuntu/{vpu}_zarr_complete.txt', 'w') as f:
                f.write('completed')
            return
    except Exception as e:
        with open(f'/home/ubuntu/{vpu}_zarr_error.txt', 'w') as f:
            f.write(str(e))


if __name__ == '__main__':
    vpus = natsorted(glob(f'{discharge_root}/*'))
    vpus = [d for d in vpus if os.path.isdir(d)]
    vpus = [os.path.basename(vpu) for vpu in vpus]

    completed_routing = natsorted(glob(f'{discharge_root}/*/finalstate_202412312300.parquet'))
    completed_routing = [os.path.basename(os.path.dirname(c)) for c in completed_routing]
    completed_conversion = [os.path.basename(t).split('_')[0] for t in glob('/home/ubuntu/*_zarr_complete.txt')]

    skip = []
    skip = [f'vpu={s}' for s in skip]
    vpus = [vpu for vpu in vpus if vpu not in skip]
    vpus = [vpu for vpu in vpus if vpu in completed_routing]
    vpus = [vpu for vpu in vpus if vpu not in completed_conversion]
    vpus = natsorted(vpus)

    with Pool(6) as p:  # 3x24 maxes out a 96 vCPU @ 8GB/vCPU machine
        for v in vpus:
            p.apply_async(convert, args=(v,))
        p.close()
        p.join()

    print('opening zarrs')
    ds = xr.concat([xr.open_zarr(z) for z in natsorted(glob(f'{zarr_root}/*.zarr'))], dim='river_id')
    ds.attrs = {
        "title": "River Forecast System v2 Hourly Retrospective Simulation",
        "description": "Hourly simulation of global rivers since 1940 based on TDX-Hydro hydrography, ERA5 meteorology reanalysis, and matrix Muskingum vector routing using river-route.",
        "author": "Riley Chad Hales, PhD",
        "creation_date": "2025-02-22",
        "license": "CC-BY-NC-SA 4.0",
        "copyright": "2025",
        "revision": "1"
    }
    print('writing zarr')
    (
        ds
        .chunk({'time': -1, 'river_id': 20})
        .to_zarr(
            f'{final_root}/hourly.zarr',
            mode='w',
            zarr_format=2,
            compute=True,
            consolidated=True,
            encoding={
                'Q': {'compressor': numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.AUTOSHUFFLE)}
            },
        )
    )
    with open(f'/home/ubuntu/zarr-hourly-complete', 'w') as f:
        f.write('complete')
