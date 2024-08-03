import boto3
import json
import netCDF4 as nc
import os
import pwinput
import requests
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# URL of the website where files are located
base_url = 'https://home.chpc.utah.edu/~u1260390/TA/latest_forecast/'
# Path to the folder containing the input .nc files
folder_path = 'ncfiles/'

# Function to download files
def get_files(mapbox_username, mapbox_access_token):
    dates = [datetime.now() + timedelta(days=i) for i in range(6)]
    first_date = dates[0]
    last_date = dates[-1]
    
    with ThreadPoolExecutor(max_workers=16) as executor:  # Adjust max_workers as needed
        futures = []
        for date in dates:
            datestring = date.strftime('%Y-%m-%d')
            
            if date == first_date:
                for i in range(6, 24):
                    time = str(i).zfill(2)
                    futures.append(executor.submit(download_file, datestring, time, mapbox_username, mapbox_access_token))
            
            elif date == last_date:
                for i in range(0, 6):
                    time = str(i).zfill(2)
                    futures.append(executor.submit(download_file, datestring, time, mapbox_username, mapbox_access_token))
            
            else:
                for i in range(0, 24):
                    time = str(i).zfill(2)
                    futures.append(executor.submit(download_file, datestring, time, mapbox_username, mapbox_access_token))
        
        for future in as_completed(futures):
            try:
                future.result()  # Ensure all tasks complete before proceeding
            except Exception as e:
                print(f"Exception occurred: {e}")
                

def download_file(datestring, time, mapbox_username, mapbox_access_token):
    net_cdf_filename = f'{datestring}_{time}.nc'
    file_url = base_url + net_cdf_filename
    response = requests.get(file_url)
    net_cdf_file_path = os.path.join(folder_path, net_cdf_filename)

    if response.status_code == 200:
        with open(net_cdf_file_path, 'wb') as f:
            f.write(response.content)
        print(f'{net_cdf_filename} downloaded.')

        geojson_filename = net_cdf_filename.replace('.nc', '.geojson')
        geojson_file_path = os.path.join(folder_path, geojson_filename)

        nc_to_geojson(net_cdf_file_path, net_cdf_filename, geojson_file_path, geojson_filename)
        os.remove(net_cdf_file_path)

        mbtiles_filename = geojson_filename.replace('.geojson', '.mbtiles')
        mbtiles_file_path = os.path.join(folder_path, mbtiles_filename)

        geojson_to_mbtiles(geojson_file_path, geojson_filename, mbtiles_file_path, mbtiles_filename)
        os.remove(geojson_file_path)

        upload_mbtiles_to_mapbox(mbtiles_filename, mbtiles_file_path, mapbox_username, mapbox_access_token)
        os.remove(mbtiles_file_path)

    else:
        print(f'Failed to download: {net_cdf_filename}')

def nc_to_geojson(net_cdf_file_path, net_cdf_filename, geojson_file_path, geojson_filename):
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
    print(f"{net_cdf_filename} converted to {geojson_filename}.")

def geojson_to_mbtiles(geojson_file_path, geojson_filename, mbtiles_file_path, mbtiles_filename):
    tippe_canoe_command = f"tippecanoe -o {mbtiles_file_path} -l PM25 -zg --drop-fraction-as-needed {geojson_file_path}"
    subprocess.run(tippe_canoe_command, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"{geojson_filename} converted to {mbtiles_filename}.")

def upload_mbtiles_to_mapbox(mbtiles_filename, mbtiles_file_path, mapbox_username, mapbox_access_token):
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
    else:
        print(f"{mbtiles_filename} uploaded to Mapbox.")

if __name__ == '__main__':
    mapbox_username = input('Input Mapbox username: ')
    mapbox_access_token = pwinput.pwinput('Input Mapbox access token: ')

    get_files(mapbox_username, mapbox_access_token)
    print("All files processed and uploaded.")