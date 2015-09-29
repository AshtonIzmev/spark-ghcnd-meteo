# spark-ghcnd-meteo
## Build weather features using weather stations data

## Weather data
* Download the weather data from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/readme.txt
* Add a header to each file : id,date,type,v1,v2,v3,v4,v5

## Weather stations
* Use the station referential from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
* Remove unnecessary blanks and keep only the three useful columns : ids, lattitude and longitude


You can compute weather features using this code 
