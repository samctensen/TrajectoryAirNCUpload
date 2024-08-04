# Trajectory Air NetCDF Conversion Tool

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
    git clone git@github.com:samctensen/TrajectoryAirNCUpload.git
    ```
    or
    * Clone Project with HTTPS:
    ```
    git clone https://github.com/samctensen/TrajectoryAirNCUpload.git
    ```
* Run the Python Environment Install Script:
  ```
  ./install.sh
  ```

### Running the Upload Script
Now that everything is installed, you can run the upload script.
* Enter the ```/TrajectoryAirNCUpload/``` Repository Folder:
    ```
    cd TrajectoryAirNCUpload
    ```
* Run the Python Environment Upload Script:
  ```
  ./upload.sh
  ```
* When the script begins, the user will be prompted to enter their MapBox username.
* After entering the username, the user will be prompted to enter their MapBox token. This token needs both the TILESETS:READ and TILESETS:WRITE permissions.
* After entering the MapBox token, the uploading process will begin. Once the process is complete, the upload script will end with the message:
"All files processed and uploaded."
