#!/bin/bash

volumes_root="/mnt/volumes"
discharge_root="/mnt/discharge"
zarr_root="/mnt/zarr/hourly"
final_root="/mnt/zarr/final"
configs_root="/home/ubuntu/routing_configs"

discharge_root="/mnt/discharge/MAXES"

if [ "$1" == "-d" ] || [ "$1" == "-v" ]; then
  if [ "$1" == "-d" ]; then
    selected_directory=$discharge_root
    search_string="Q_hourly*.nc"
    search_string="Q_monmax*.nc"
  elif [ "$1" == "-v" ]; then
    selected_directory=$volumes_root
    search_string="volumes_*.nc"
  fi
  find "$selected_directory" -type d | sort | while read -r dir; do
      count=$(find "$dir" -maxdepth 1 -type f -name "$search_string" | wc -l)
      if [ $count -eq 1020 ]; then
          echo "$dir: $count -- Complete"
      else
          echo "$dir: $count"
      fi
  done
  total_files=$(find $selected_directory -name "$search_string" | wc -l)
  total_files_expected=127500
  percent_complete=$(echo "scale=5; $total_files / $total_files_expected * 100" | bc)
  echo "Total Files: $total_files"
  echo "Total Files for Completion: $total_files_expected"
  echo "Percent Complete: $percent_complete"

elif [ "$1" == "-z" ]; then
# get a list of all the directories in configs_root
  find "$configs_root" -type d | sort | while read -r dir; do
    if [ -d "$zarr_root/$(basename $dir).zarr" ]; then
      echo "$dir: Found"
    else
      echo "$dir: ---Missing---"
    fi
  done
  # print a status message with the number of directories found
  expected_directories=$(find $configs_root -type d | wc -l)
  total_directories=$(find $zarr_root -maxdepth 1 -type d | wc -l)
  echo "Expected Directories: $expected_directories"
  echo "Total Directories: $total_directories"
  echo "Percent Complete: $(echo "scale=5; $total_directories / $expected_directories * 100" | bc)"

elif [ "$1" == "-f" ]; then
  for zarr in hourly daily monthly-timeseries monthly-timesteps yearly-timeseries yearly-timesteps maximums; do
    if [ -d "$final_root/$zarr.zarr" ]; then
      echo "$final_root/$zarr.zarr: Found"
    else
      echo "$final_root/$zarr.zarr: ---Missing---"
    fi
  done
else
    echo "Invalid flag. Please use -v, -d, -z, -f"
    exit 1
fi