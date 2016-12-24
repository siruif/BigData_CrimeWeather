case
#!/bin/bash
mkdir crimeData
cd crimeData
wget https://data.cityofchicago.org/api/views/c4ep-ee5m/rows.csv?accessType=DOWNLOAD

mv rows.csv\?accessType\=DOWNLOAD crimeData.csv
