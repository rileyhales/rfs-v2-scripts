import os
import shutil
from glob import glob
from multiprocessing import Pool

import pandas as pd
from natsort import natsorted

import river_route as rr

configs_root = '/home/ubuntu/routing_configs'
volumes_root = '/mnt/volumes'
runoffs_root = '/mnt/era5'


def compute_volumes(arg):
    configs = arg[0]
    runoff_file = arg[1]
    weight_table_name = f'gridweights_ERA5_{os.path.basename(configs)}.nc'
    volumes_dir = os.path.join(volumes_root, os.path.basename(configs))
    os.makedirs(volumes_dir, exist_ok=True)

    try:
        rr.runoff.write_catchment_volumes(
            rr.runoff.calc_catchment_volumes(
                runoff_data=runoff_file,
                weight_table=os.path.join(configs, weight_table_name),
                runoff_var='ro',
                x_var='longitude',
                y_var='latitude',
                river_id_var='river_id',
                time_var='valid_time',
                cumulative=False,
                force_positive_runoff=True,
                force_uniform_timesteps=False,
            ),
            output_dir=volumes_dir,
        )
        print(f'Job done for {configs} and {runoff_file}')
    except Exception as e:
        print(f'Error in {configs} and {runoff_file}: {e}')
    return


if __name__ == '__main__':
    n_configs_duplicates = 20
    all_configs = [f'routing_configs_{n + 1}' for n in range(n_configs_duplicates)]
    print(f'duplicating configs root')
    for c in all_configs:
        if not os.path.exists(f'/home/ubuntu/{c}'):
            shutil.copytree(configs_root, f'/home/ubuntu/{c}')

    # Collect configs from all duplicates
    configs_dirs = natsorted(glob(os.path.join(configs_root, '*')))
    configs_dirs = [d for d in configs_dirs if os.path.isdir(d)]
    # sort the configs dirs large to small by the number of rows in configs/routing_parameters.parquet
    configs_dirs = sorted(configs_dirs,key=lambda x: -pd.read_parquet(os.path.join(x, 'routing_parameters.parquet')).shape[0])
    runoff_files = natsorted(glob(os.path.join(runoffs_root, 'year=*/*.nc')))

    # make all permutations of configs_dirs and runoff_files
    jobs = [[c, r] for c in configs_dirs for r in runoff_files]  # sequential by runoff file date then by vpu dir


    # check for completed files at {volumes_root}/{vpu}/volumes_{runoff_file_name}*.nc
    def output_file_exists(arg):
        file_name = f'volumes_{os.path.basename(arg[1]).split('_')[1].split('.')[0]}*.nc'
        file_path = os.path.join(volumes_root, os.path.basename(arg[0]), file_name)
        return len(glob(file_path)) > 0


    jobs = [j for j in jobs if not output_file_exists(j)]
    print(f'Jobs to complete: {len(jobs)}')

    # edit the jobs to use sequentially repeating configs dir duplicates to reduce competition for file IO
    jobs = [
        [c.replace('routing_configs', all_configs[i % n_configs_duplicates]), r]
        for i, (c, r) in enumerate(jobs)
    ]

    # Process jobs in parallel
    with Pool(92) as p:
        [p.apply_async(compute_volumes, args=(job,)) for job in jobs]
        p.close()
        p.join()
