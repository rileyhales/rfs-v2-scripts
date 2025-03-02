import os
from glob import glob
from multiprocessing import Pool

import netCDF4 as nc
import pandas as pd
from natsort import natsorted

import river_route as rr

configs_root = '/home/ubuntu/routing_configs'
volumes_root = '/mnt/volumes'
discharge_root = '/mnt/discharge/MAXES'
os.makedirs(discharge_root, exist_ok=True)


def route(configs):
    vpu = os.path.basename(configs)
    params_file = f'{configs}/routing_parameters.parquet'
    params_modified = f'{configs}/routing_parameters_faster.parquet'
    connectivity_file = f'{configs}/connectivity.parquet'
    os.makedirs(f'{discharge_root}/{vpu}', exist_ok=True)

    def custom_write_outflows(df: pd.DataFrame, outflow_file: str, runoff_file: str) -> None:
        # hourly_file = outflow_file.replace('Q_', 'Q_hourly_')
        # with nc.Dataset(hourly_file, mode='w', format='NETCDF4') as ds:
        #     ds.createDimension('time', size=df.shape[0])
        #     ds.createDimension('river_id', size=df.shape[1])
        #     time_var = ds.createVariable('time', 'f8', ('time',))
        #     time_var.units = f'seconds since {df.index[0].strftime("%Y-%m-%d %H:%M:%S")}'
        #     time_var[:] = df.index.values - df.index.values[0]
        #     id_var = ds.createVariable('river_id', 'i4', ('river_id',), )
        #     id_var[:] = df.columns.values
        #     flow_var = ds.createVariable('Q', 'f4', ('time', 'river_id'), zlib=True, complevel=3)
        #     flow_var[:] = df.values
        #     flow_var.long_name = 'Discharge at catchment outlet'
        #     flow_var.standard_name = 'discharge'
        #     flow_var.aggregation_method = 'mean'
        #     flow_var.units = 'm3 s-1'

        max_file = outflow_file.replace('Q_', 'Q_monmax_')
        df_max = df.groupby(df.index.strftime('%Y%m01')).max()
        df_max.index = pd.to_datetime(df_max.index)
        with nc.Dataset(max_file, mode='w', format='NETCDF4') as ds:
            ds.createDimension('time', size=df_max.shape[0])
            ds.createDimension('river_id', size=df_max.shape[1])
            time_var = ds.createVariable('time', 'f8', ('time',))
            time_var.units = f'seconds since {df.index[0].strftime("%Y-%m-%d %H:%M:%S")}'
            time_var[:] = df_max.index.values - df_max.index.values[0]
            id_var = ds.createVariable('river_id', 'i4', ('river_id',), )
            id_var[:] = df_max.columns.values
            flow_var = ds.createVariable('Q', 'f4', ('time', 'river_id'), zlib=True, complevel=3)
            flow_var[:] = df_max.values
            flow_var.long_name = 'Discharge at catchment outlet'
            flow_var.standard_name = 'discharge'
            flow_var.aggregation_method = 'max'
            flow_var.units = 'm3 s-1'

        # daily_file = outflow_file.replace('Q_', 'Q_daily_')
        # df = df.groupby(df.index.strftime('%Y%m%d')).mean()
        # df.index = pd.to_datetime(df.index)
        # with nc.Dataset(daily_file, mode='w', format='NETCDF4') as ds:
        #     df.index = pd.to_datetime(df.index)
        #     ds.createDimension('time', size=df.shape[0])
        #     ds.createDimension('river_id', size=df.shape[1])
        #     time_var = ds.createVariable('time', 'f8', ('time',))
        #     time_var.units = f'seconds since {df.index[0].strftime("%Y-%m-%d %H:%M:%S")}'
        #     time_var[:] = df.index.values - df.index.values[0]
        #     id_var = ds.createVariable('river_id', 'i4', ('river_id',), )
        #     id_var[:] = df.columns.values
        #     flow_var = ds.createVariable('Q', 'f4', ('time', 'river_id'), zlib=True, complevel=3)
        #     flow_var[:] = df.values
        #     flow_var.long_name = 'Discharge at catchment outlet'
        #     flow_var.standard_name = 'discharge'
        #     flow_var.aggregation_method = 'mean'
        #     flow_var.units = 'm3 s-1'

        # monthly_file = outflow_file.replace('Q_', 'Q_monthly_')
        # df = df.groupby(df.index.strftime('%Y%m01')).mean()
        # df.index = pd.to_datetime(df.index)
        # with nc.Dataset(monthly_file, mode='w', format='NETCDF4') as ds:
        #     ds.createDimension('time', size=df.shape[0])
        #     ds.createDimension('river_id', size=df.shape[1])
        #     time_var = ds.createVariable('time', 'f8', ('time',))
        #     time_var.units = f'seconds since {df.index[0].strftime("%Y-%m-%d %H:%M:%S")}'
        #     time_var[:] = df.index.values - df.index.values[0]
        #     id_var = ds.createVariable('river_id', 'i4', ('river_id',), )
        #     id_var[:] = df.columns.values
        #     flow_var = ds.createVariable('Q', 'f4', ('time', 'river_id'), zlib=True, complevel=3)
        #     flow_var[:] = df.values
        #     flow_var.long_name = 'Discharge at catchment outlet'
        #     flow_var.standard_name = 'discharge'
        #     flow_var.aggregation_method = 'mean'
        #     flow_var.units = 'm3 s-1'
        return

    for decade in range(1940, 2030, 10):
        first3 = str(decade)[:3]
        volumes = natsorted(glob(os.path.join(volumes_root, vpu, f'volumes_{first3}*.nc')))
        outflows = [os.path.join(discharge_root, vpu, os.path.basename(f).replace('volumes', 'Q')) for f in volumes]

        final_state_file = f'{discharge_root}/{vpu}/finalstate_202412312300.parquet' \
            if decade == 2020 else f'{discharge_root}/{vpu}/finalstate_{decade + 9}12312300.parquet'
        initial_state_file = '' if decade == 1940 else f'{discharge_root}/{vpu}/finalstate_{decade - 1}12312300.parquet'
        if os.path.exists(final_state_file):
            print(f'Skipping {vpu}: {decade} final state already exists')
            continue
        if initial_state_file != '' and not os.path.exists(initial_state_file):
            print(f'Skipping {vpu}: {decade} initial state does not exist')
            continue
        if len(volumes) != 120 and decade != 2020:
            print(f'Skipping {vpu}: {decade} volumes not found')
            continue
        if len(volumes) != 60 and decade == 2020:
            print(f'Skipping {vpu}: {decade} volumes not found')
            continue
        print(f'Routing {vpu} decade {decade}')
        (
            rr
            .Muskingum(**{
                'routing_params_file': params_modified,
                'connectivity_file': connectivity_file,
                'catchment_volumes_file': volumes,
                'outflow_file': outflows,
                'initial_state_file': initial_state_file,
                'final_state_file': final_state_file,
                'dt_routing': 3600,
                'progress_bar': False,
                'log_stream': f'{discharge_root}/{vpu}/log_{decade}.log',
            })
            .set_write_outflows(custom_write_outflows)
            .route()
        )
        print(f'Finished routing {vpu} decade {decade}')


if __name__ == '__main__':
    configs_dirs = natsorted(glob(os.path.join(configs_root, '*')))
    configs_dirs = [d for d in configs_dirs if os.path.isdir(d)]

    completed_volumes = natsorted(glob(os.path.join(volumes_root, '*')))
    completed_volumes = [v for v in completed_volumes if os.path.isdir(v)]
    completed_volumes = [os.path.basename(v) for v in completed_volumes]

    complete_routing = natsorted(glob(os.path.join(discharge_root, '*', 'finalstate_202412312300.parquet')))
    complete_routing = [os.path.dirname(c) for c in complete_routing]

    skip = []
    skip = [f'vpu={s}' for s in skip]
    configs_dirs = [c for c in configs_dirs if os.path.basename(c) not in skip]
    configs_dirs = [c for c in configs_dirs if os.path.basename(c) in completed_volumes]
    configs_dirs = [c for c in configs_dirs if os.path.basename(c) not in complete_routing]
    configs_dirs = sorted(configs_dirs, key=lambda x: -pd.read_parquet(f'{x}/routing_parameters.parquet').shape[0])

    if len(configs_dirs) == 0:
        print('No routing to do')
    elif len(configs_dirs) == 1:
        route(configs_dirs[0])
    else:
        with Pool(min([len(configs_dirs), 92])) as p:
            p.map(route, configs_dirs)
            p.close()
            p.join()
