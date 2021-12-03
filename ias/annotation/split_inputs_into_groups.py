import argparse
from tqdm import tqdm
import numpy as np

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.struct_pb2 import Struct


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
  input_ids = []

  last_id = ''
  while True:
    # Make request
    req = service_pb2.StreamInputsRequest(per_page=1000, last_id=last_id)
    response = stub.StreamInputs(req, metadata=args.metadata)
    process_response(response)

    # Process inputs
    if len(response.inputs) == 0:
      break
    else:
      for input in response.inputs:
        input_ids.append(input.id)

    # Set id for next stream
    last_id = response.inputs[-1].id

  print(f"Total of {len(input_ids)} inputs retrieved")
  return input_ids


def split_into_groups(args, input_ids):
  n_groups = args.num_labelers // args.per_group # TODO: consult Michael
  split = np.array_split(input_ids, n_groups)
  print(f"Inputs were split in {n_groups} groups")
  return split


def add_groups_to_metadata(args, split):
  for i, group in enumerate(split):
    print(f"Processing group {i+1}...")

    # Add group to metadata and patch each input in the group
    for input_id in tqdm(group, total=len(group)):
      input_metadata = Struct()
      input_metadata.update({"group": str(i+1)})
      response = stub.PatchInputs(
        service_pb2.PatchInputsRequest(
          action="merge", 
          inputs=[
            resources_pb2.Input(
              id=input_id,
              data=resources_pb2.Data(metadata=input_metadata)
            )
          ]
        ),
        metadata=args.metadata
      )
      process_response(response)


def main(args):

  print("----- Spliting inputs into groups for labeling task scheduling -----")

  # Fetch ids of inputs in the app
  input_ids = get_input_ids(args)

  # Split inputs into groups depending on input parameters
  split = split_into_groups(args, input_ids)

  # Patch inputs to add groups to metadata
  add_groups_to_metadata(args, split)

  print("Done!")


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Split inputs into groups for labeling.") 
  parser.add_argument('--api_key',
                      default='',
                      required=True,
                      help="API key of the application.")                   
  parser.add_argument('--num_labelers', 
                      default=20, 
                      type=int,
                      required=True,
                      help="Total number of available labelers.") 
  parser.add_argument('--per_group', 
                      default=5, 
                      type=int,
                      help="Number of labelers per group") 

  args = parser.parse_args()
  args.metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args)