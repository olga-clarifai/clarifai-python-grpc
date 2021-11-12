##############################################################################
### Code from experimental/john/ias
##############################################################################

import csv
import requests

url_list = []

with open("ground_english.csv", "r") as f:
  csvfile = csv.reader(f, delimiter=",")
  next(csvfile)
  next(csvfile)
  for row in csvfile:
    url_list.append(row[3])
    

for url in url_list:
  print("FILE URL {}".format(url))
  file_name=url.split("=")[-1]
  r = requests.get(url, allow_redirects=True)
  open("english/" + file_name + ".mp4", 'wb').write(r.content)