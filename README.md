# v2-reporting-label-extractor
Unique container label extractor for Sysdig v2 Reporting

Expectations:
v1-report.csv is already downloaded and available on the filesystem using previously existing methods

Step 1:
get_sysdig_v2_reports.py = example to list, download, and extract v2 report to the current directory

Step 2:
csv_processor.py = using the v2-report.csv file from the previous step extract a specific namespace label and write a dictionary of unique image id's and namespace labels to output.csv

Step 3:
merge-report.py = using the dictionary of image id's and namespace labels in output.csv, open up v1-report.csv and merge image id/namespace label matches to a new column in the report called merged-report.csv

