import argparse
import os
from tqdm import tqdm
import numpy as np

import ground_truth as gt
from taxonomy import GT_SAFE_LABEL

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict


# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def process_response(response):
  if response.status.code != status_code_pb2.SUCCESS:
    print("There was an error with your request!")
    print(f"\tDescription: {response.status.description}")
    print(f"\tDetails: {response.status.details}")
    raise Exception(f"Request failed, status code: {response.status.code}")


def get_input_ids(args):
  print("Retrieving inputs...")
  input_ids = {}

  last_id = ''
  while True:
    # Make request
    req = service_pb2.StreamInputsRequest(per_page=1000, last_id=last_id)
    response = stub.StreamInputs(req, metadata=args.input_metadata)
    process_response(response)

    # Process inputs
    if len(response.inputs) == 0:
      break
    else:
      for input in response.inputs:
        hosted = input.data.video.hosted
        input_ids[input.id] = {'url': f"{hosted.prefix}/orig/{hosted.suffix}",
                               'video_id': MessageToDict(input.data.metadata)['id'],
                               'metadata': input.data.metadata}

    # Set id for next stream
    last_id = response.inputs[-1].id

#   # # ------ DEBUG CODE
#   input_ids_ = {}
#   for id in list(input_ids.keys())[0:5]:
#     input_ids_[id] = input_ids[id]
#   input_ids = input_ids_
#   print("Number of selected inputs: {}".format(len(input_ids)))
#   # # ------ DEBUG CODE

  print(f"Total of {len(input_ids)} inputs retrieved")
  return input_ids


def add_ground_truth(args, input_ids):
  assert os.path.exists(args.ground_truth), f"Ground truth file {args.ground_truth} doesn't exist"
  ground_truth, no_gt_count = gt.load_from_csv(input_ids, args.ground_truth, GT_SAFE_LABEL)

  for input_id in input_ids:
    input_ids[input_id]['ground_truth'] = ground_truth[input_id]

  return input_ids


def upload_data(args, input_ids):
  for input_id in tqdm(input_ids, total=len(input_ids)):
    post_inputs_response = stub.PostInputs(
      service_pb2.PostInputsRequest(
        inputs=[
          resources_pb2.Input(
            id = input_id,
            data=resources_pb2.Data(
              video=resources_pb2.Video(url=input_ids[input_id]['url']),
              metadata=input_ids[input_id]['metadata']
            )
          )
        ]
      ),
      metadata=args.output_metadata
    )
    process_response(post_inputs_response)
  print("Uploaded sucessfully.")

def main(args):

  print("----- Spliting inputs into groups for labeling task scheduling -----")

  # Fetch ids of inputs in the app
  input_ids = get_input_ids(args)
  if args.ground_truth != '':
    input_ids = add_ground_truth(args, input_ids)
  upload_data(args, input_ids)
  print("Done!")


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Split inputs into groups for labeling.") 
  parser.add_argument('--input_api_key',
                      default='')     
  parser.add_argument('--output_api_key',
                      default='') 
  parser.add_argument('--ground_truth', 
                      default='', 
                      help="Path to csv file with ground truth if available.")                  
  args = parser.parse_args()
  args.input_metadata = (('authorization', 'Key {}'.format(args.input_api_key)),)
  args.output_metadata = (('authorization', 'Key {}'.format(args.output_api_key)),)

  main(args)