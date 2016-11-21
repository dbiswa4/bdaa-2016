Dataset Name:
Airplane Crashes Since 1908

Dataset Source:
https://www.kaggle.com/saurograndi/airplane-crashes-since-1908

Dataset Description:
Full history of airplane crashes throughout the world, from 1908-2009.
Dataset has below columns:
Date :
Time :
Location :
Operator :
Flight # :
Route :
Type :
Registration :
cn/In :
Aboard :
Fatalities :
Ground :
Summary :



Data cleanup and data generation for charts:

1. Remove null from data
2. Added Year field in dataset
3. Prepare the dataset for line chart with "year" and "count of accident". Line chart will show accident per year.
4. Prepare the dataset for histogram with "year" and "Fatalities". Histogram will show "Aircrash Fatalities vs Number of Aircrash"


Python Scripts:

download.py: It will download the dataset in zip file and unzip the file and put it in 


