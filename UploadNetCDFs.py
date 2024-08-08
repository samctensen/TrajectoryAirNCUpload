import boto3
import json
import netCDF4 as nc
import os
import pwinput
import requests
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone


# URL of the website where files are located
base_url = 'https://home.chpc.utah.edu/~u1260390/TA/latest_forecast/'
# Path to the folder containing the input .nc files
folder_path = 'ncfiles/'

# Function to clear old files in the ncfiles directory
def clear_directory():
    for file in os.listdir(folder_path):
        delete_file_path = os.path.join(folder_path, file)
        os.remove(delete_file_path)

# Function very mapbox username and token before starting
def verify_credentials(mapbox_username, mapbox_access_token):
    mapbox_credentials_url = f'https://api.mapbox.com/uploads/v1/{mapbox_username}/credentials?access_token={mapbox_access_token}'
    valid_mapbox_credentials = requests.post(mapbox_credentials_url).ok
    if valid_mapbox_credentials:
        print("Mapbox token authenticated.")
        return True
    else:
        print("Invalid Mapbox username or token. Please try again.")
        return False
        
# Parent function, schedules individial download jobs.
def download_files() -> list:
    dates = [datetime.now() + timedelta(days=i) for i in range(6)]
    first_date = dates[0]
    last_date = dates[-1]
    net_cdfs_filenames = []
    futures = []
    completed_jobs = 0
    total_jobs = sum(24 if date not in (first_date, last_date) else (18 if date == first_date else 6) for date in dates)
    print(f"\r({completed_jobs}/{total_jobs}) .nc forecast files downloaded.", end="")
    
    with ThreadPoolExecutor(max_workers=16) as executor:  # Adjust max_workers as needed
        for date in dates:
            datestring = date.strftime('%Y-%m-%d')
            if date == first_date:
                for i in range(6, 24):
                    time = str(i).zfill(2)
                    net_cdf_filename = f'{datestring}_{time}.nc'
                    futures.append(executor.submit(download_file, net_cdf_filename))
            elif date == last_date:
                for i in range(0, 6):
                    time = str(i).zfill(2)
                    net_cdf_filename = f'{datestring}_{time}.nc'
                    futures.append(executor.submit(download_file, net_cdf_filename)) 
            else:
                for i in range(0, 24):
                    time = str(i).zfill(2)
                    net_cdf_filename = f'{datestring}_{time}.nc'
                    futures.append(executor.submit(download_file, net_cdf_filename))

        for future in as_completed(futures):
            try:
                net_cdf_filename = future.result()
                if net_cdf_filename:
                    net_cdfs_filenames.append(net_cdf_filename)
            except Exception as e:
                print(f"Exception occurred: {e}")
            completed_jobs += 1
            print(f"\r({completed_jobs}/{total_jobs}) .nc forecast files downloaded.", end="")
            sys.stdout.flush()

    return net_cdfs_filenames

# Child function, downloads a single forecast file.
def download_file(net_cdf_filename: str) -> (str | None):
    file_url = base_url + net_cdf_filename
    response = requests.get(file_url)
    net_cdf_file_path = os.path.join(folder_path, net_cdf_filename)
    if response.status_code == 200:
        with open(net_cdf_file_path, 'wb') as f:
            f.write(response.content)
        return net_cdf_filename
    else:
        print(f'Failed to download: {net_cdf_filename}')
        return None

# Parent function, schedules individial nc to geojson jobs.
def ncs_to_geojsons(net_cdf_filenames: list) -> list:
    geojson_filenames = []
    futures = []
    completed_jobs = 0
    total_jobs = len(net_cdf_filenames)
    print(f"\r({completed_jobs}/{total_jobs}) .nc files converted to .geojson.", end="")
    
    with ThreadPoolExecutor(max_workers=16) as executor:  # Adjust max_workers as needed
        for net_cdf_filename in net_cdf_filenames:
            futures.append(executor.submit(nc_to_geojson, net_cdf_filename))

        for future in as_completed(futures):
            try:
                geojson_filename = future.result()  # Retrieve the result from the future
                if geojson_filename:
                    geojson_filenames.append(geojson_filename)
            except Exception as e:
                print(f"Exception occurred: {e}")
            completed_jobs += 1
            print(f"\r({completed_jobs}/{total_jobs}) .nc files converted to .geojson.", end="")
            sys.stdout.flush()

    return geojson_filenames
            
# Child function, converts individual nc files to geojson.
def nc_to_geojson(net_cdf_filename: str) -> str:
    geojson_filename = net_cdf_filename.replace('.nc', '.geojson')
    net_cdf_file_path = os.path.join(folder_path, net_cdf_filename)
    geojson_file_path = os.path.join(folder_path, geojson_filename)

    # Open the NetCDF file
    ds = nc.Dataset(net_cdf_file_path, 'r')

    # Extract longitude, latitude, and PM25 data
    lon = ds.variables['lon'][:]
    lat = ds.variables['lat'][:]
    pm25 = ds.variables['PM25'][:]

    features = []
    feature_id = 1
    # Iterate over the lon, lat, and PM25 arrays and combine them into features
    for i in range(len(lon)):
        for j in range(len(lat)):
            pm25_value = float(pm25[j, i])
            # Filter PM25 values between 8 and 200
            if 5 <= pm25_value:
                feature = {
                    "type": "Feature",
                    "properties": {
                        "id": str(feature_id),
                        "PM25": pm25_value
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(lon[i]), float(lat[j]), 0]
                    }
                }
                features.append(feature)
                feature_id += 1

    # Create the final GeoJSON structure
    geojson_header = {
        "type": "FeatureCollection",
        "crs": {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
            }
        }
    }

    # Start writing the JSON file
    with open(geojson_file_path, 'w') as f:
        f.write('{\n')
        f.write('"type": "FeatureCollection",\n')
        f.write('"crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },\n')
        f.write('"features": [\n')

        # Write each feature in one line
        for idx, feature in enumerate(features):
            feature_str = json.dumps(feature)
            if idx < len(features) - 1:
                feature_str += ','
            f.write(feature_str + '\n')

        # End the JSON file
        f.write(']\n')
        f.write('}\n')

    # Close the NetCDF file
    ds.close()
    os.remove(net_cdf_file_path)
    return geojson_filename

# Parent function, schedules individial geojson to mbtile jobs.
def geojsons_to_mbtiles(geojson_filenames: list) -> list:
    mbtile_filenames = []
    futures = []
    completed_jobs = 0
    total_jobs = len(geojson_filenames)
    print(f"\r({completed_jobs}/{total_jobs}) .geojson files converted to .mbtiles.", end="")

    with ThreadPoolExecutor(max_workers=16) as executor:  # Adjust max_workers as needed
        for geojson_filename in geojson_filenames:
            futures.append(executor.submit(geojson_to_mbtiles, geojson_filename))

        for future in as_completed(futures):
            try:
                mbtile_filename = future.result()  # Retrieve the result from the future
                if mbtile_filename:
                    mbtile_filenames.append(mbtile_filename)
            except Exception as e:
                print(f"Exception occurred: {e}")
            completed_jobs += 1
            print(f"\r({completed_jobs}/{total_jobs}) .geojson files converted to .mbtiles.", end="")
            sys.stdout.flush()

    return mbtile_filenames

# Child function, converts individual geojson files to mbtiles.
def geojson_to_mbtiles(geojson_filename: str) -> str:
    geojson_file_path = os.path.join(folder_path, geojson_filename)
    mbtiles_filename = geojson_filename.replace('.geojson', '.mbtiles')
    mbtiles_file_path = os.path.join(folder_path, mbtiles_filename)
    tippe_canoe_command = f"tippecanoe -o {mbtiles_file_path} -l PM25 -zg --drop-fraction-as-needed {geojson_file_path}"
    subprocess.run(tippe_canoe_command, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove(geojson_file_path)
    return mbtiles_filename

# Parent function, schedules individial mbtile upload jobs.
def upload_mbtiles_to_mapbox(mbtiles_filenames: list, mapbox_username: str, mapbox_access_token: str):
    futures = []
    completed_jobs = 0
    total_jobs = len(mbtiles_filenames)
    print(f"\r({completed_jobs}/{total_jobs}) .mbtiles uploaded to MapBox.", end="")

    with ThreadPoolExecutor(max_workers=16) as executor:  # Adjust max_workers as needed
        for mbtiles_filename in mbtiles_filenames:
            futures.append(executor.submit(upload_mbtile_file_to_mapbox, mbtiles_filename, mapbox_username, mapbox_access_token))

        for future in as_completed(futures):
            try:
                future.result()  # Retrieve the result from the future
            except Exception as e:
                print(f"Exception occurred: {e}")
            completed_jobs += 1
            print(f"\r({completed_jobs}/{total_jobs}) .mbtiles uploaded to MapBox.", end="")
            sys.stdout.flush()

# Child function, uploads individual mbtile files to Mapbox.
def upload_mbtile_file_to_mapbox(mbtiles_filename: str, mapbox_username: str, mapbox_access_token: str):
    mbtiles_file_path = os.path.join(folder_path, mbtiles_filename)
    # Delete existing tileset if it exists
    tileset_id = f"{mapbox_username}.{mbtiles_filename.removesuffix('.mbtiles')}"
    mapbox_delete_tileset_url = f'https://api.mapbox.com/tilesets/v1/{tileset_id}?access_token={mapbox_access_token}'
    requests.delete(mapbox_delete_tileset_url)

    # Get Mapbox credentials
    mapbox_credentials_url = f'https://api.mapbox.com/uploads/v1/{mapbox_username}/credentials?access_token={mapbox_access_token}'
    mapbox_credentials_response = requests.post(mapbox_credentials_url)
    mapbox_credentials = mapbox_credentials_response.json()

    # Upload the tileset to AWS S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=mapbox_credentials['accessKeyId'],
        aws_secret_access_key=mapbox_credentials['secretAccessKey'],
        aws_session_token=mapbox_credentials['sessionToken'],
    )
    bucket = mapbox_credentials['bucket']
    key = mapbox_credentials['key']
    s3.upload_file(mbtiles_file_path, bucket, key)

    # Upload S3 tileset to Mapbox Studio
    upload_url = f'https://api.mapbox.com/uploads/v1/{mapbox_username}?access_token={mapbox_access_token}'
    upload_payload = {
        "url": f"https://{bucket}.s3.amazonaws.com/{key}",
        "tileset": tileset_id,
        "name": f"{mbtiles_filename.removesuffix('.mbtiles')}"
    }
    response = requests.post(upload_url, json=upload_payload)
    upload_response = response.json()
    if upload_response['error']:
        print("Error uploading tileset to Mapbox.")
    os.remove(mbtiles_file_path)

# Deletes tilesets that are older than 5 hours
def clear_depreciated_tilesets(mapbox_username: str, mapbox_access_token: str):
    print("Removing depriciated tilesets.")
    all_tilesets_url = f'https://api.mapbox.com/tilesets/v1/{mapbox_username}?access_token={mapbox_access_token}&limit=500&sortby=modified'
    response = requests.get(all_tilesets_url)
    tilesets = json.loads(response.text)
    current_time = datetime.now(timezone.utc)
    for i in range(len(tilesets) - 1, -1, -1):
        index = int("{:03}".format(i))
        tileset = tilesets[index]
        date_modified = datetime.strptime(tileset['modified'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
        time_threshold = current_time - timedelta(hours=5)
        # Tileset is older than 5 hours, delete
        if date_modified < time_threshold:
            tileset_id = tilesets[index]['id']
            mapbox_delete_tileset_url = f'https://api.mapbox.com/tilesets/v1/{tileset_id}?access_token={mapbox_access_token}'
            requests.delete(mapbox_delete_tileset_url)
        # Tileset is too new to delete
        else:
            break
    print("All files processed and uploaded.")

if __name__ == '__main__':
    mapbox_username = input('Input Mapbox username: ')
    mapbox_access_token = pwinput.pwinput('Input Mapbox access token: ')
    clear_directory()
    valid_auth = verify_credentials(mapbox_username, mapbox_access_token)
    if valid_auth:
        net_cdf_filenames = download_files()
        geojson_filenames = ncs_to_geojsons(net_cdf_filenames)
        mbtile_filenames = geojsons_to_mbtiles(geojson_filenames)
        upload_mbtiles_to_mapbox(mbtile_filenames, mapbox_username, mapbox_access_token)
        clear_depreciated_tilesets(mapbox_username, mapbox_access_token)
    