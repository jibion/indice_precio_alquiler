# Rent Price Index (Spain)

This Python script creates different MySQL tables with public data, from the Spanish Ministry of Transportation, Mobility and Urban Agenda, related with the rent price of homes in Spain (at different administrative levels). Those tables simplify the advanced querying of data. 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

Python libraries used

* os
* decouple
* requests
* pandas
* numpy
* logging
* sqlalchemy
* janitor

### Installation

Clone the repository (or donwload the file).

Change the values of the variables under ## Configuration

```
# Defining our connection variables
username = 'user' # replace with your username
password = 'pwd' # replace with your password
ipaddress = 'localhost' # change this to your db’s IP address
port = 3306 # this is the standard port for MySQL, but change it to your port if needed
dbname = 'database' # change this to the name of your db
```


## Usage

Simply execute the script as usual.

```
python spanish_index_home_rent.py
```

* If this is the first time you execute the script, the tables will be created in the pointed database.

*If the tables have being already created, you are given the possibility to drop and recreate the tables. This is usefull in case of new data being published.

## Additional Documentation and Acknowledgments

* More information about the origin of the data (only in Spanish): https://www.mitma.gob.es/vivienda/alquiler/indice-alquiler
