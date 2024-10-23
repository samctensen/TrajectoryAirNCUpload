# Trajectory Air Upload Tool
This Trajectory Air repository uploads the 120 daily updated .nc files as vector tilesets to MapBox Studio. It first converts the .nc files to the commonly used .geojson file format before using MapBox's own [TippeCannoe](https://github.com/mapbox/tippecanoe) tool to convert the .geojson files to .mbtiles. These tilesets are uploaded to MapBox studio where they can be accessed visually at [TrajectoryAir](https://trajectory-air.vercel.app/).

### Installation
Before running, you need to install homebrew, tippecanoe, and clone the repo.
* Install Homebrew:
    ```
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ```
  * Pay attention during install and run the commands in the terminal it requires after the initial install.
* Install TippeCanoe Tileset Creator:
    ```
    brew install tippecanoe
    ```
* Clone the Repo:
    * Clone Project with SSH:
    ```
    git clone git@github.com:samctensen/TrajectoryAirUploader.git
    ```
    or
    * Clone Project with HTTPS:
    ```
    git clone https://github.com/samctensen/TrajectoryAirUploader.git
    ```
* Enter the ```/TrajectoryAirUploader/``` Repository Folder:
    ```
    cd TrajectoryAirUploader
    ```
* Run the Python Environment Install Script:
  ```
  ./install.sh
  ```

### Running the Upload Script
Now that everything is installed, you can run the upload script.
* Enter the ```/TrajectoryAirUploader/``` Repository Folder:
    ```
    cd TrajectoryAirUploader
    ```
* Run the Python Environment Upload Script:
  ```
  ./upload.sh
  ```
* When the script begins, the user will be prompted to enter their MapBox username.
* After entering the username, the user will be prompted to enter their MapBox token. This token needs both the TILESETS:READ and TILESETS:WRITE permissions.
* After entering the MapBox token, the uploading process will begin. Once the process is complete, the upload script will end with the message:
"All files processed and uploaded."
