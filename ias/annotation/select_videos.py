
import csv
import random
import json

from os import listdir
from os.path import isfile, join
onlyfiles = [f for f in listdir("english/") if isfile(join("english/", f))]

url_list = []
class_list = {"safe":[]}
lst=[]


with open("ground_english2.csv", "r") as f:
  csvfile = csv.reader(f, delimiter=",")
  i = 0
  for row in csvfile:
    if i == 0:
      class_list[row[5]] = []
      class_list[row[6]] = []
      class_list[row[7]] = []
      class_list[row[8]] = []
      class_list[row[9]] = []
      class_list[row[10]] = []
      class_list[row[11]] = []
      class_list[row[12]] = []
      class_list[row[13]] = []
      class_list[row[14]] = []
      class_list[row[15]] = []
      lst.append(row[5])
      lst.append(row[6])
      lst.append(row[7])
      lst.append(row[8])
      lst.append(row[9])
      lst.append(row[10])
      lst.append(row[11])
      lst.append(row[12])
      lst.append(row[13])
      lst.append(row[14])
      lst.append(row[15])

      i = 1
      continue

    file_name = row[3].split("=")[-1] + ".mp4"
    # print(file_name)
    # print(onlyfiles[0])
    if file_name in onlyfiles:
      if row[5] == "1":
        class_list[lst[0]].append(row[3])
        continue
      if row[6] == "1":
        class_list[lst[1]].append(row[3])
        continue
      if row[7] == "1":
        class_list[lst[2]].append(row[3])
        continue
      if row[8] == "1":
        class_list[lst[3]].append(row[3])
        continue
      if row[9] == "1":
        class_list[lst[4]].append(row[3])
        continue
      if row[10] == "1":
        class_list[lst[5]].append(row[3])
        continue
      if row[11] == "1":
        class_list[lst[6]].append(row[3])
        continue
      if row[12] == "1":
        class_list[lst[7]].append(row[3])
        continue
      if row[13] == "1":
        class_list[lst[8]].append(row[3])
        continue
      if row[14] == "1":
        class_list[lst[9]].append(row[3])
        continue
      if row[15] == "1":
        class_list[lst[10]].append(row[3])
        continue
      if row[5] == "0" and row[6] == "0" and row[7] == "0" and row[8] == "0" and row[9] == "0" and row[10] == "0" and row[11] == "0" and row[12] == "0" and row[13] == "0" and row[14] == "0" and row[15] == "0":
        class_list["safe"].append(row[3])
      
with open("final_list_non_redundant.json", "w") as f:
  json.dump(class_list, f)
# print(class_list)
print("-------")
total = 0
for cat,ent in class_list.items():
  print("{}: {}".format(cat, len(ent)))
  total = total + len(ent)
print("-----")
print("Total count: {}".format(total))
final_list = {}
print("-------")
for cat, ent in class_list.items():
  if cat == "safe":
    l = random.sample(ent, 100)
  elif cat == "adult_&_explicit_sexual_content":
    l = random.sample(ent, 100)
  elif cat == "obscenity_&_profanity":
    l = random.sample(ent, 100)
  elif cat == "illegal_drugs/tobacco/e-cigarettes/vaping/alcohol":
    l = random.sample(ent, 100)
  else:
    if len(ent) >= 50:
      l = random.sample(ent, 50)
    else:
      l = ent
  final_list[cat] = l

  with open("experiment_group1.json", "w") as f:
    json.dump(final_list, f)
