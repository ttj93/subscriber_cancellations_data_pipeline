#!/bin/bash

# get python3 location
PYTHON_BIN=$(command -v python3)

# Check if Python is found
if [ -z "$PYTHON_BIN" ]; then
    echo "Error: python3 not found ."
    exit 1
fi

# Set working directory
WORKING_DIR="$(pwd)/dev"
PRODUCTION_DIR="$(pwd)/prod"


scriptcontinue=0
# Prompts the user to confirm before cleansing the data
echo "Ready to clean the data? [1/0]"
read cleancontinue

# If the user selects 1 then run Python clean_data script
if [ $cleancontinue -eq 1 ]
then
    echo "Cleaning Data"
    python "${WORKING_DIR}/script.py"
    echo "Done Cleaning Data"

    # Grab the first line of dev and prod changelogs
    dev_version=$(head -n 1 dev/changelog.md)
    prod_version=$(head -n 1 prod/changelog.md)

    # Delimit the 1st line of the changelog by space
    read -a splitversion_dev <<< $dev_version
    read -a splitversion_prod <<< $prod_version

    # Current versions
    echo "DEV version: '$dev_version'"
    echo "PROD version: '$prod_version'"
    # Delimiting will result in a list of two elements
    # The second (index 1) will contain the version numbers
    dev_version=${splitversion_dev[1]}
    prod_version=${splitversion_prod[1]}

    # Check if an update occurred based on logs
     if [ "$prod_version" != "$dev_version" ]
    then
        # Tells the user that the dev and prod changes are different
        # ask before proceeding to move dev files to prod
        echo "New changes detected. Move dev files to prod? [1/0]"
        read scriptcontinue

    fi
# Otherwise, don't run anything
else
    echo "Please come back when you are ready"
fi

# If user selects 1 then copy or move certain files from dev to prod
if [ $scriptcontinue -eq 1 ]
then
    for filename in dev/*
    do
        if [ $filename == "dev/cademycode_ds.db" ] || [ $filename == "dev/cademycode_ds.csv" ] || [ $filename == "dev/changelog.md" ]
        then
            # Copy updated files to production directory
            cp "$filename" "$PRODUCTION_DIR"
            echo "Copying " $filename
        else
            echo "Not Copying " $filename
        fi
    done
# Otherwise, don't run anything
else
    echo "Please come back when you are ready"
fi

read -p "Press Enter to close this window..."