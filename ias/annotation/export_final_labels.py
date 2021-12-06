import argparse
import os
import csv
from tqdm import tqdm

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct


# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def process_response(response):
    if response.status.code != status_code_pb2.SUCCESS:
        print("There was an error with your request!")
        print("\tDescription: {}".format(response.status.description))
        print("\tDetails: {}".format(response.status.details))
        raise Exception("Request failed, status code: " + str(response.status.code))


def get_input_metadata(args):
  ''' Get list of all inputs (ids of videos that were uploaded) from the app '''

  print("Retrieving labels...")
  input_meta = {}
  last_id = ''

  while True:
    # Make request
    req = service_pb2.StreamInputsRequest(per_page=100, last_id=last_id)
    response = stub.StreamInputs(req, metadata=args.metadata)
    process_response(response)

    # Process inputs
    if len(response.inputs) == 0:
      break
    else:
      for input in response.inputs:
        # Extract input's metadata
        meta_ = input['data']['metadata']
        meta = {'video_id': meta_['video_id'],
                'description': meta_['description'], 
                'url': meta_['url'],
                'final_labels': meta_['final_labels']
        }
        input_meta[input.id] = meta

    # Set id for next stream
    last_id = response.inputs[-1].id

  print(f"Number of retrieved inputs: {len(input_meta)}")
  return input_meta


def save_annotations_csv(args, input_meta):
    ''' Dump fetched labels to a csv file '''
    
    # Create output dir if needed
    path = os.path.dirname(args.output_file)
    if not os.path.exists(path):
        os.mkdirs(path)

    # Create header
    label_names = sorted(list(list(input_meta.values())[0]['final_labels'].keys()))
    header = ['video_id', 'description', 'url'] + label_names

    # Write to file
    with open(args.output_file, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)

        # Dump labels for every input
        for input in input_meta.values():
            info = [input['video_id'], input['description'], input['url']]
            labels = [input['final_labels'][name] for name in label_names]
            writer.writerow(info + labels)

    print("Labels saved to {}".format(args.output_file))


def main(args, metadata):
  # Get input ids
  input_meta = get_input_metadata(args)

  # Write labels to csv file
  save_annotations_csv(args, input_meta)


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Run tracking.")
  parser.add_argument('--api_key',
                      default='',
                      help="API key of the application.")  
  parser.add_argument('--output_file', 
                      default='', 
                      help="Full path (including name and .csv extension), where to export annotations.")                           
                    
  args = parser.parse_args()
  args.metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args)