#!/bin/bash

# Create a python virtual environment
python3 -m venv .

# Initialize the virtual environment
source bin/activate

# Install the required packages
pip install -r requirements.txt

# Create directory for the NetCDF files
mkdir ncfiles

# Create directory for the NumPy files
mkdir npyfiles