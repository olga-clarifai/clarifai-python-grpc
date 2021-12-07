import argparse
import os
import json

# Import the Clarifai gRPC based objects
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf import json_format


# Construct a communication channel and a stub object to call requests on
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def process_response(response):
    if response.status.code != status_code_pb2.SUCCESS:
        print("There was an error with your request!")
        print("\tDescription: {}".format(response.status.description))
        print("\tDetails: {}".format(response.status.details))
        raise Exception("Request failed, status code: " + str(response.status.code))


def get_input_metadata(args):
  ''' Get metadata of all videos from the app '''

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
        meta_ = input.data.metadata

        # Format final labels
        final_labels = json_format.MessageToDict(meta_['final_labels'])
        final_labels = dict(sorted(final_labels.items()))

        # Store relevant metadata
        meta = {'video_description': meta_['description'], 
                'video_url': meta_['url'],
                'final_labels': final_labels
        }
        input_meta[meta_['video_id']] = meta

    # Set id for next stream
    last_id = response.inputs[-1].id

  print(f"Number of inputs retrieved from the app: {len(input_meta)}")
  return input_meta


def save_annotations_json(args, input_meta):
    ''' Dump fetched labels to a json file '''

    # Create output directory if needed
    path = os.path.dirname(args.output_file)
    if not os.path.exists(path):
      os.makedirs(path)

    # Write to file
    with open(args.output_file, 'w') as f:
      json.dump(input_meta, f)

    print("Labels saved to {}".format(args.output_file))


def main(args):
  
  # Get input ids
  input_meta = get_input_metadata(args)
  # Write labels to json file
  save_annotations_json(args, input_meta)


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Export final labels to a json file.")
  parser.add_argument('--api_key',
                      default='',
                      help="API key of the application.")  
  parser.add_argument('--output_file', 
                      default='', 
                      help="Full path (including name and .json extension), where to export labels.")                           
                    
  args = parser.parse_args()
  args.metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args)