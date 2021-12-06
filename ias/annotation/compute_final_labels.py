import argparse
import utils
from tqdm import tqdm

from save_annotations import add_final_labels_to_metadata

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def get_input_ids(metadata):
  ''' Get list of all inputs (ids of videos that were uploaded) from the app '''

  input_ids = {}
  # Get inputs
  for page in range(1,11):
    list_inputs_response = stub.ListInputs(
                          service_pb2.ListInputsRequest(page=page, per_page=1000),
                          metadata=metadata
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
      # if 'group' in meta_:
      #   meta['group'] = meta_['group']
      # if 'take' in meta_:
      #   meta['take'] = meta_['take']

      input_ids[json_obj['id']] = meta
  
  logger.info("Input ids fetched. Number of fetched inputs: {}".format(len(input_ids)))

  # # ------ DEBUG CODE
  # input_ids_ = {}
  # for id in list(input_ids.keys())[0:50]:
  #   input_ids_[id] = input_ids[id]
  # input_ids = input_ids_
  # print("Number of selected inputs: {}".format(len(input_ids)))
  # # ------ DEBUG CODE

  return input_ids, len(input_ids)


def get_annotations(args, metadata, input_ids):
  ''' Get list of annotations for every input id'''

  logger.info("Retrieving annotations...")

  annotations = {} # list of concepts
  annotations_meta = {} # store metadata

  # Get annotations for every input id
  for input_id in tqdm(input_ids, total=len(input_ids)):
    list_annotations_response = stub.ListAnnotations(
                                service_pb2.ListAnnotationsRequest(
                                input_ids=[input_id], 
                                per_page=35,
                                list_all_annotations=True
                                ),
      metadata=metadata
    )
    utils.process_response(list_annotations_response)
    # TODO: make requests in batches

    meta_ = []

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
        meta_.append((concept, ao['userId']))

    # Remove duplicates from meta, transform it into dictionary (for further convenience) and store
    meta = set(meta_)
    meta = [{'concept': m[0], 'userId': m[1]} for m in meta]
    annotations_meta[input_id] = meta

    # Extract concepts only
    if args.broad_consensus:
      user_annotations = {}
      for m in meta_:
        # Extract '2-' only
        if '2-' in m[0]:
          annotation = m[0] if m[0] == args.safe_annotation else m[0][0:4] # TODO: eliminate 0:4 notation
          if m[1] in user_annotations:
            user_annotations[m[1]].append(annotation)
          else:
            user_annotations[m[1]] = [annotation]

      # Eliminate duplicate concepts within a user      
      annotation = []
      for user in user_annotations:
        annotation += list(set(user_annotations[user]))
      annotations[input_id] = annotation

    else:
      annotations[input_id] = [m['concept'] for m in meta if '2-' in m['concept']]

  n_annotations = sum([1 for a in annotations if annotations[a]])

  logger.info("Annotations fetched. Number of annotated inputs".format(n_annotations))
  return annotations, annotations_meta


def aggregate_annotations(args, input_ids, annotations):
  ''' Count the number of different annotation labels for every input id'''

  aggregated_annotations = {}
  not_annotated_count = 0

  for input_id in input_ids:
    # Aggregate
    aggregation = {a:annotations[input_id].count(a) for a in annotations[input_id]}
    if not aggregation:
        not_annotated_count += 1
    else:
      aggregated_annotations[input_id] = aggregation

  logger.info("Annotations aggregated. Number of not annotated inputs".format(not_annotated_count))

  return aggregated_annotations, not_annotated_count


def compute_consensus(args, input_ids, aggregated_annotations):
  ''' Compute consensus among annotations for every input id based on aggregation'''

  # Variable to count how many times no full consensus has been reached
  no_consensus_count = 0

  def consesus_fun(value):
    return True if value >= 3 else False

  consensus = {}
  for input_id in input_ids:
      if input_id in aggregated_annotations:
        # Compute consensus
        aa = aggregated_annotations[input_id]
        consensus_exists = {k:consesus_fun(v) for k, v in aa.items()}

        # If no consensus exists, set consensus to None
        if not True in consensus_exists.values():
          consensus[input_id] = None
          no_consensus_count += 1
          continue

        # If conflict between consenuses exist,
        # keep only positive consensus
        if any(v for k, v in consensus_exists.items() if k in args.positive_annotations) and \
           args.safe_annotation in consensus_exists and consensus_exists[args.safe_annotation]:
            # Save only consensus for positive annotations
            consensus_exists.pop(args.safe_annotation)
       
        # Store consensus
        consensus[input_id] = [concept for concept, exists in consensus_exists.items() if exists]

  logger.info("Consensus computed. {} annotation do not have consensus".format(no_consensus_count))
  return consensus, no_consensus_count


def assign_classes(args, input_ids, consensus):
  ''' Assign class to each input/category pair: P (positive), N (negative), S (safe) '''

  classes = {}
  for input_id in input_ids:
    
    # Assign classes
    if input_id in consensus:
      classes_ = {}
      for annotation in args.positive_annotations:
        if consensus[input_id] is None:
          classes_[annotation] = 'N'
        elif annotation in consensus[input_id]:
          classes_[annotation] = 'P'
        elif args.safe_annotation in consensus[input_id]:
          classes_[annotation] = 'S'
      classes[input_id] = classes_
    else:
      classes[input_id] = None

  logger.info("Classes assigned.")
  return classes


def patch_metadata(args, input_ids):

    for input_id in tqdm(input_ids, total=len(input_ids)):
      input_metadata = Struct()
      # input_metadata.update({'final_labels': input_ids[input_id]['final_labels']})
      input_metadata.update(input_ids[input_id])
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
        metadata=metadata
      )
      utils.process_response(response)
    
    logger.info("Successfully patched metadata.")


def main(args, metadata):

  logger.info("----- Patching final labels for {} -----".format(args.tag))

  # Get input ids
  input_ids, input_count = get_input_ids(metadata)

  # Get annotations for every id together with their aggregations
  annotations, annotations_meta = get_annotations(args, metadata, input_ids)
  aggregated_annotations, not_annotated_count = aggregate_annotations(args, input_ids, annotations)

  # Compute consensus
  consensus, no_consensus_count = compute_consensus(args, input_ids, aggregated_annotations)

  # Assign positive, negative or safe class according to consensus
  classes = assign_classes(args, input_ids, consensus)

  # Add final labels to inputs metadata
  input_ids = add_final_labels_to_metadata(input_ids, classes)
  patch_metadata(args, input_ids)



if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Run tracking.")
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.") 
  parser.add_argument('--group',
                      default='Hate_Speech',
                      choices={'Hate_Speech', 'Group_1'},
                      help="Name of the group.")  
  parser.add_argument('--tag',
                      default='',
                      help="Name of the process/application.")                  
  parser.add_argument('--broad_consensus',
                      default=True,
                      type=lambda x: (str(x).lower() == 'true'),
                      help="Attempt to allow for a broad consensus (i.e. multiple hate speech labels all pool to hate speech.")                
                    

  args = parser.parse_args()
  args.tag = args.tag + '_' + args.group

  metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  # Hate Speech
  if args.group == 'Hate_Speech':
    args.positive_annotations = ['2-HB']
    args.safe_annotation = '2-not-hate'

  # Group 1
  elif args.group == 'Group_1':
    args.positive_annotations = ['2-AD', '2-OP', '2-ID']
    args.safe_annotation = '2-none-of-the-above'

  main(args, metadata)