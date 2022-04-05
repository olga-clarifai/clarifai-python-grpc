import argparse
import os
import requests
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
    print(response)
    print("There was an error with your request!")
    print(f"\tDescription: {response.status.description}")
    print(f"\tDetails: {response.status.details}")
    raise Exception(f"Request failed, status code: {response.status.code}")


def get_ids(metadata):
  print("Retrieving inputs...")
  ids = {}

  last_id = ''
  while True:
    # Make request
    req = service_pb2.StreamInputsRequest(per_page=1000, last_id=last_id)
    response = stub.StreamInputs(req, metadata=metadata)
    process_response(response)

    # Process inputs
    if len(response.inputs) == 0:
      break
    else:
      for input in response.inputs:
        ids[input.id] = MessageToDict(input.data.metadata)['id']

    # Set id for next stream
    last_id = response.inputs[-1].id

  print(f"Total of {len(ids)} inputs retrieved")
  return ids



def main(args):

  # Fetch ids of inputs in the app
  input_ids = get_ids(args.input_metadata)
  output_ids = get_ids(args.output_metadata)

  print(f'Mismatched input ids: {len(set(input_ids.keys()) - set(output_ids.keys()))}')
  print(f'Mismatched video ids: {len(set(input_ids.values()) - set(output_ids.values()))}')

  print('Done')

if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Split inputs into groups for labeling.") 
  parser.add_argument('--input_api_key',
                      default='')     
  parser.add_argument('--output_api_key',
                      default='')                   
  args = parser.parse_args()
  args.input_metadata = (('authorization', 'Key {}'.format(args.input_api_key)),)
  args.output_metadata = (('authorization', 'Key {}'.format(args.output_api_key)),)

  main(args)