import argparse
import utils
import os
from tqdm import tqdm
import pandas as pd

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import service_pb2, service_pb2_grpc
from google.protobuf.json_format import MessageToDict

# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def get_input_ids(args):
  ''' Get list of all inputs (ids of videos that were uploaded) from the app '''

  input_ids = {}
  # Get inputs
  for page in range(1,13):
    list_inputs_response = stub.ListInputs(
                          service_pb2.ListInputsRequest(page=page, per_page=1000),
                          metadata=args.metadata
    )
    utils.process_response(list_inputs_response)

    # Process inputs in response
    for input_object in list_inputs_response.inputs:
      json_obj = MessageToDict(input_object)

      # Correct input's meta and store it
      meta_ = json_obj['data']['metadata']
      meta = {}
      meta['video_id'] = meta_['video_id'] if 'video_id' in meta_ else meta_['id']
      meta['description'] = meta_['description'] if 'description' in meta_ else meta_['video_description']
      meta['url'] = meta_['url'] if 'url' in meta_ else meta_['video_url']
      input_ids[json_obj['id']] = meta
  
  logger.info("Input ids fetched. Number of fetched inputs: {}".format(len(input_ids)))

  # # ------ DEBUG CODE
  # input_ids_ = {}
  # for id in list(input_ids.keys())[200:250]:
  #   input_ids_[id] = input_ids[id]
  # input_ids = input_ids_
  # print("Number of selected inputs: {}".format(len(input_ids)))
  # # ------ DEBUG CODE

  return input_ids


def get_annotations(args, input_ids):
  ''' Get list of annotations for every input id'''

  annotations = {input_id: [] for input_id in input_ids} # list of concepts

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
    # TODO: make requests in batches

    # Loop through all annotations
    for annotation_object in list_annotations_response.annotations:
      ao = MessageToDict(annotation_object)

      # Check for concepts in data but also time segments
      concepts = []
      if 'concepts' in ao['data'] and len(ao['data']['concepts'])>0:
        concepts.append(ao['data']['concepts'][0]['name'])
      elif 'timeSegments' in ao['data'] and 'concepts' in ao['data']['timeSegments'][0]['data'] and \
           len(ao['data']['timeSegments'][0]['data'])>0:
        concepts.append(ao['data']['timeSegments'][0]['data']['concepts'][0]['name'])

      # Get meta about all found concepts besides duplicates
      for concept in concepts:
        annotations[input_id].append((concept, ao['userId']))

    # Remove duplicates
    annotations[input_id] = set(annotations[input_id])

  logger.info("Annotations fetched.")
  return annotations


def count_annotations(input_ids, annotations):
  ''' Count number of annotations per concept '''

  concepts = []
  for input_id in input_ids:
    concepts_count = {}
    for annotation in annotations[input_id]:
      concepts.append(annotation[0])
      if annotation[0] in concepts_count.keys():
        concepts_count[annotation[0]] += 1
      else:
        concepts_count[annotation[0]] = 1
    annotations[input_id] = concepts_count
  return annotations, sorted(list(set(concepts)))


def save_output(args, input_ids, annotations, concepts):
  ''' Transform raw concept counts into an output data frame and save it '''

  # Create output dir if needed
  path = os.path.join(args.out_path, 'granular_labels')
  if not os.path.exists(path):
      os.mkdir(path)

  df = pd.DataFrame()
  df['video_id'] = input_ids.keys()
  df['video_description'] = [value['description'] for input_id, value in input_ids.items()]
  df['video_url'] = [value['url'] for input_id, value in input_ids.items()]

  for concept in concepts:
    concept_col = []
    for input_id in input_ids:
      if concept in annotations[input_id].keys():
        concept_col.append(annotations[input_id][concept])
      else:
        concept_col.append(0)
    df[concept] = concept_col

  df.to_csv(os.path.join(path, f'{args.tag}_granular_labels.csv'), index=False)
  logger.info("Annotations saved.")

def main(args):

  input_ids = get_input_ids(args)
  annotations = get_annotations(args, input_ids)
  annotations, concepts = count_annotations(input_ids, annotations)
  save_output(args, input_ids, annotations, concepts)


if __name__ == '__main__':  
  parser = argparse.ArgumentParser()
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.")  
  parser.add_argument('--tag',
                      default='',
                      help="Name of the application.")                                
  parser.add_argument('--out_path', 
                      default='', 
                      help="Path to general output directory for this script.")

  args = parser.parse_args()
  args.metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args)