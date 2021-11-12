
import json
import csv

from google.protobuf.struct_pb2 import Struct

## Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_pb2, status_code_pb2

channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)

metadata = (('authorization', 'Key '),)

video_ids_mapping = {}
description_mapping = {}
sfl = {}

with open("ground_english2.csv", "r") as fl:
  csvfile = csv.reader(fl, delimiter=",")
  next(csvfile)
  for row in csvfile:
    video_ids_mapping[row[3]] = row[1]
    description_mapping[row[3]] = row[2]
    sfl[row[3]] = row[0]


with open("experiment_group1_2.json", "r") as j:
  data = json.load(j)

cat_list = ["safe", 
"adult_&_explicit_sexual_content",
"arms_&_ammunition",
"crime",
"death,_injury_or_military_conflict",
"online_piracy",
"hate_speech",
"obscenity_&_profanity",
"illegal_drugs/tobacco/e-cigarettes/vaping/alcohol",
"spam_or_harmful_content",
"terrorism",
"debated_sensitive_social_issue"]

failed_list = []
for cat, v_list in data.items():
  print("Uploading category {} : {}".format(cat, len(v_list)))
  for video_file in v_list:
    video_id_to_upload = video_ids_mapping[video_file]
    video_description = description_mapping[video_file]
    video_sfl = sfl[video_file]

    input_metadata = Struct()
    input_metadata.update({"description": video_description, "source-file-line": video_sfl, "id": video_id_to_upload, "url": video_file, "results": {cat: True} })
    
    file_name = "english/" + video_file.split("=")[-1] + ".mp4"

    print("\tUploading {} : {}".format(file_name, video_id_to_upload))

    with open(file_name, "rb") as f:
      file_bytes = f.read()

      post_inputs_response = stub.PostInputs(
          service_pb2.PostInputsRequest(
              inputs=[
                  resources_pb2.Input(
                      data=resources_pb2.Data(
                          video=resources_pb2.Video(
                              base64=file_bytes
                          ),
                          metadata=input_metadata
                      )
                  )
              ]
          ),
          metadata=metadata
      )

      if post_inputs_response.status.code != status_code_pb2.SUCCESS:
          print("There was an error with your request!")
          print("\tCode: {}".format(post_inputs_response.outputs[0].status.code))
          print("\tDescription: {}".format(post_inputs_response.outputs[0].status.description))
          print("\tDetails: {}".format(post_inputs_response.outputs[0].status.details))
          # raise Exception("Post inputs failed, status: " + post_inputs_response.status.description)
          failed_list.append(video_file)

print(failed_list)
with open("failed_list.txt", "w") as flist:
  flist.write(failed_list)