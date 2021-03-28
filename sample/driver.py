# Extract covid data
# Extract financial data
import os
import extract
import url

county_path = 'tmp/county'
#os.makedirs(county_path)



#extract.California(county_path, 'california', 'csv', 'w', url.california_url)
#extract.Texas(county_path, 'texas', 'xlsx', 'wb', url.texas_url)
extract.Florida(county_path, 'florida', 'json', 'w')


# Clean covid data
# Clean financial data


# Store covid data
# Clean financial data


