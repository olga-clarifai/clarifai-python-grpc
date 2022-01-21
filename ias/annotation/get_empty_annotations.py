import argparse
import utils
from tqdm import tqdm
import json
import os


# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import service_pb2, service_pb2_grpc
from google.protobuf.json_format import MessageToDict

# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)

SPECIAL_USE_LABELS = {'1-CT-nottarget-or-english', '1-CT-dontunderstand-english', '1-video-unavailable'}

def get_input_ids(args):
  ''' Get list of all input ids from the app '''

  input_ids = []
  # Get inputs
  for page in range(1,13):
    list_inputs_response = stub.ListInputs(
                          service_pb2.ListInputsRequest(page=page, per_page=1000),
                          metadata=args.metadata
    )
    utils.process_response(list_inputs_response)

    # Process inputs in response
    for input_object in list_inputs_response.inputs:
      input_ids.append(input_object.id)
  
  logger.info(f"Input ids fetched. Number of fetched inputs: {len(input_ids)}")

  # # ------ DEBUG CODE
  # input_ids = input_ids[0:500]
  # print(f"Number of selected inputs: {len(input_ids)}")
  # # ------ DEBUG CODE

  return input_ids


def get_annotations(args, input_ids):
  ''' Get list of annotations for every input id'''

  annotations_meta = {}
  users = []

  # Get annotations for every input id
  for input_id in tqdm(input_ids, total=len(input_ids)):
    list_annotations_response = stub.ListAnnotations(
                                service_pb2.ListAnnotationsRequest(
                                input_ids=[input_id], 
                                per_page=35,
                                list_all_annotations=True
                                ),
      metadata=args.metadata
    )
    utils.process_response(list_annotations_response)

    meta = []

    # Loop through all annotations
    for annotation_object in list_annotations_response.annotations:

      # print(annotation_object)

      ao = MessageToDict(annotation_object)

      concept = None
      # Check for concepts in data but also time segments
      if 'concepts' in ao['data'] and len(ao['data']['concepts'])>0:
        concept = ao['data']['concepts'][0]['name']
      elif 'timeSegments' in ao['data'] and 'concepts' in ao['data']['timeSegments'][0]['data'] and \
           len(ao['data']['timeSegments'][0]['data'])>0:
        concept = ao['data']['timeSegments'][0]['data']['concepts'][0]['name']

      if 'userId' in ao:
        # Get meta about all found concepts besides duplicates
        meta.append((ao['userId'], concept, ao['status']['code']))
        users.append(ao['userId'])

    annotations_meta[input_id] = meta

  logger.info("Annotations fetched.")

  empty_annotations = {}
  for input_id in input_ids:
    meta = annotations_meta[input_id]
    empty_count = 0
    for m in meta:
      if m[0] != 'ias':
        # if m[1] == None and m[2] == 'ANNOTATION_AWAITING_CONSENSUS_REVIEW':
        if m[1] == None and m[2] == 'ANNOTATION_PENDING':
          empty_count += 1
    empty_annotations[input_id] = empty_count

  missing_1 = sum(1 for input_id in input_ids if empty_annotations[input_id] == 1)
  missing_2 = sum(1 for input_id in input_ids if empty_annotations[input_id] == 2)
  missing_3 = sum(1 for input_id in input_ids if empty_annotations[input_id] == 3)
  missing_4 = sum(1 for input_id in input_ids if empty_annotations[input_id] == 4)
  missing_5 = sum(1 for input_id in input_ids if empty_annotations[input_id] == 5)

  empty_annotations_total = sum(1 for input_id in input_ids if empty_annotations[input_id] > 0)

  print('---------- Empty annotations ---------')
  print(f'Total number of partially empty inputs: {empty_annotations_total}')
  print(f'Number of inputs missing 1 annotation: {missing_1}')
  print(f'Number of inputs missing 2 annotations: {missing_2}')
  print(f'Number of inputs missing 3 annotations: {missing_3}')
  print(f'Number of inputs missing 4 annotations: {missing_4}')
  print(f'Number of inputs missing 5 annotations: {missing_5}')

  users = list(set(users) - {'ias'}) # unique users
  user_annotations = {user: {} for user in users}
  for input_id in input_ids:
    meta = annotations_meta[input_id]
    for m in meta:
      if m[0] != 'ias':
        if input_id in user_annotations[m[0]]:
          user_annotations[m[0]][input_id].append((m[1], m[2]))
        else:
          user_annotations[m[0]][input_id] = [(m[1], m[2])]

  empty_user_annotations = {user: {} for user in users}
  for user in user_annotations:
    for input_id in input_ids:
      if input_id in user_annotations[user].keys():
        annotations = user_annotations[user][input_id]
        empty = not any(True if a[0] is not None else False for a in annotations)
        if empty:
          empty_user_annotations[user][input_id] = [a[1] for a in annotations]

  print('---------- Empty annotations per user ---------')
  empty_user_annotations_count = {user: len(empty_user_annotations[user].keys()) for user in empty_user_annotations}
  empty_user_annotations_count = dict(sorted(empty_user_annotations_count.items(), key=lambda x: x[1], reverse=True))
  for user in empty_user_annotations_count:
    print(f'{user}: {empty_user_annotations_count[user]}')

  # with open(os.path.join(args.out_path, f'{args.tag}_empty.json'), 'w') as json_file:
  #   json.dump(empty_user_annotations, json_file)

  no_basis_user_annotations = {user:{} for user in users}
  for user in user_annotations:
    for input_id in input_ids:
      if input_id in user_annotations[user].keys():
        annotations = user_annotations[user][input_id]
        no_basis = not any(True if a[0] is not None and '2-' in a[0] else False for a in annotations)
        if no_basis:
          no_basis_user_annotations[user][input_id] = [(a[0], a[1]) for a in annotations if a[0] is None or '2-' not in a[0]] 

  # with open(os.path.join(args.out_path, f'{args.tag}_no_basis.json'), 'w') as json_file:
  #   json.dump(no_basis_user_annotations, json_file)

  print('---------- Content-only annotations per user ---------')
  only_content_user_annotations_count = {user:{} for user in users}
  for user in user_annotations:
    only_content_user_annotations_count[user] = len(no_basis_user_annotations[user].keys()) - empty_user_annotations_count[user]
  only_content_user_annotations_count = dict(sorted(only_content_user_annotations_count.items(), key=lambda x: x[1], reverse=True))
  for user in only_content_user_annotations_count:
    print(f'{user}: {only_content_user_annotations_count[user]}')

  special_user_annotations_count = {}
  for user in user_annotations:
    special = 0
    for input_id in input_ids:
      if input_id in user_annotations[user].keys():
        annotations = user_annotations[user][input_id]
        special += any(True if a[0] is not None and a[0] in SPECIAL_USE_LABELS else False for a in annotations)
    special_user_annotations_count[user] = special

  print('---------- Special annotations per user ---------')
  special_user_annotations_count = dict(sorted(special_user_annotations_count.items(), key=lambda x: x[1], reverse=True))
  for user in special_user_annotations_count:
    print(f'{user}: {special_user_annotations_count[user]}')


def main(args):

  # Get input ids
  input_ids = get_input_ids(args)

  # Get annotations for every id
  get_annotations(args, input_ids)
  

if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Run tracking.")
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.")  
  parser.add_argument('--tag',
                      default='',
                      help="Name of the process/application.")                                
  parser.add_argument('--out_path', 
                      default='', 
                      help="Path to general output directory for this script.")

  args = parser.parse_args()
  args.metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args)