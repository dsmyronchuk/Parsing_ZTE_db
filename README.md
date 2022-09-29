# Parsing_ZTE_db

## Description

This script connects remotely to the server via the ssh protocol, passes through the required path to the zip folder in which backups from control systems for the last three days are saved. It selects the most zip folder folder by date and enters it
The zip folder contains the csv files we need with the data. We go through all the files, read their data and write them to the MySQL database
