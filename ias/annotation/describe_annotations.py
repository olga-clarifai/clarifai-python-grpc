import argparse
import utils
from tqdm import tqdm

from save_labels import save_labels_csv
from taxonomy import get_taxonomy_object, SPECIAL_USE_LABELS

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

  # ------ DEBUG CODE
  input_ids_ = {}
  for id in list(input_ids.keys())[200:250]:
    input_ids_[id] = input_ids[id]
  input_ids = input_ids_
  print("Number of selected inputs: {}".format(len(input_ids)))
  # ------ DEBUG CODE

  return input_ids, len(input_ids)


def get_annotations(args, taxonomy, input_ids):
  ''' Get list of annotations for every input id'''

  # Variable to check if number of annotations per page is sufficient
  annotation_nb_max = 0
  # Number of inputs with duplicated annotations
  duplicate_count = 0 
  duplicated_inputs = []

  annotations = {} # list of concepts
  annotations_meta = {} # store metadata
  special_labels = {} # number of labelers assigned special labels

  if utils.special_labels_present(stub, args.metadata):
    special_labels_default = None
  else:
    special_labels_default = {label: 'n/a' for label in SPECIAL_USE_LABELS.keys()}

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

    meta_ = []

    # Loop through all annotations
    for annotation_object in list_annotations_response.annotations:
      ao = MessageToDict(annotation_object)

      print(annotation_object)

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

    # Singal potential duplicates
    if len(meta_) > len(meta):
        duplicate_count += 1
        duplicated_inputs.append({'input_id': input_id, 
                                  'video_id': input_ids[input_id]['video_id'],  
                                  'duplicates': len(meta_) - len(meta)})

    # ------- Extract concepts
    user_annotations = {}
    for m in meta_:
      # Extract only category labels
      if m[0] not in taxonomy.content and m[0] not in SPECIAL_USE_LABELS:
        annotation = m[0]

        # Shorten the concept name -> determinate its category
        for category in taxonomy.categories:
          if annotation in category.positive:
            annotation = category.aggr_positive
          elif annotation in category.safe:
            annotation = category.aggr_safe

        # Add to other annotations from the same user
        if m[1] in user_annotations:
          user_annotations[m[1]].append(annotation)
        else:
          user_annotations[m[1]] = [annotation]

    # Eliminate duplicate concepts within a user      
    annotation = []
    for user in user_annotations:
      annotation += list(set(user_annotations[user]))
    annotations[input_id] = annotation

    # ------- Extract special use labels
    if special_labels_default: # special labels are not available in the app
      special_labels[input_id] = special_labels_default
    else:
      special_labels_ = {label: 0 for label in SPECIAL_USE_LABELS.keys()}
      for m in meta:
        if m['concept'] in SPECIAL_USE_LABELS:
          special_labels_[m['concept']] += 1
      special_labels[input_id] = special_labels_

    # Update max count variable
    annotation_nb_max = max(annotation_nb_max, len(list_annotations_response.annotations))

  logger.info("Annotations fetched.")
  # logger.info("\tMaximum number of annotation entries per input: {}".format(annotation_nb_max))
  # logger.info("\tNumber of annotated inputs with duplicates: {}".format(duplicate_count))

  # # ------ DEBUG CODE
  # # Print duplicates
  # print('\nInput id - Video id - Duplicates')
  # for input in duplicated_inputs:
  #   print("{} - {} - {}".format(input['input_id'], input['video_id'], input['duplicates']))
  # print('\n')
  # # ------ DEBUG CODE

  # # ------ DEBUG CODE
  # # Compute list of unique labels
  # concepts = list(itertools.chain(*[annotations[input_id] for input_id in input_ids]))
  # concepts_count = {c:concepts.count(c) for c in concepts}
  # print("Annotation concepts are: ")
  # [print("\t{}: {}".format(k, v)) for k, v in concepts_count.items()]
  # # ------ DEBUG CODE

  return annotations, annotations_meta, special_labels


def aggregate_annotations(input_ids, annotations):
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

  logger.info("Annotations aggregated.")

  # TODO: store aggregated annotations
  return aggregated_annotations, not_annotated_count


def compute_consensus(taxonomy, input_ids, aggregated_annotations):
  ''' Compute consensus among annotations for every input id based on aggregation'''

  # Variable to count how many times no full consensus has been reached
  no_consensus_count = 0
  # Varibale to store ids for inputs that had conflicting annotations in consensus
  conflict_ids = []

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
        for category in taxonomy.categories:
          if any(v for k, v in consensus_exists.items() if k == category.aggr_positive) and \
            consensus_exists.get(category.aggr_safe, False):
              # Save only consensus for positive annotations
              consensus_exists.pop(category.aggr_safe)
              conflict_ids.append(input_id)
       
        # Store consensus
        consensus[input_id] = [concept for concept, exists in consensus_exists.items() if exists]

  logger.info("Consensus computed.")
  return consensus, no_consensus_count, conflict_ids


def assign_classes(taxonomy, input_ids, consensus):
  ''' Assign class to each input/category pair: 
      _LP_ (positive), _LN_ (negative), _LS_ (safe) '''

  classes = {}
  for input_id in input_ids:
    # Assign classes
    if input_id in consensus:
      classes_ = {}
      for category in taxonomy.categories:
        if consensus[input_id] is None:
          classes_[category.name] = '_LN_'
        else:
          if category.aggr_positive in consensus[input_id]:
            classes_[category.name] = '_LP_'
          elif category.aggr_safe in consensus[input_id]:
            classes_[category.name] = '_LS_'
          else:
            classes_[category.name] = '_LN_'
      classes[input_id] = classes_
    else:
      classes[input_id] = None

  logger.info("Classes assigned.")
  return classes


def compute_totals(taxonomy, input_ids, classes):
  ''' Compute total number of inputs for each class '''

  totals = {}

  # For every category
  for category in taxonomy.categories:
    totals_ = {'_LP_': 0, '_LN_': 0, '_LS_': 0}

    for input_id in input_ids:
      if input_id in classes and classes[input_id] is not None:
        if '_LP_' in classes[input_id][category.name]:
          totals_['_LP_'] += 1
        if '_LN_' in classes[input_id][category.name]:
          totals_['_LN_'] += 1
        if '_LS_' in classes[input_id][category.name]:
          totals_['_LS_'] += 1
    totals[category.name] = totals_

  logger.info("Totals computed.")
  return totals
  

def compute_distribution(taxonomy, annotations_meta):
  ''' Compute number of occurrences for each label sub-category '''

  # Number of content and category duplicates
  duplicates_count = {'content': 0, 'category': 0}
  # Dictionary of all labels with number of their occurrences
  labels_count = {'content': {}, 'category': {}}

  for meta in annotations_meta.values():

    # Count all labels
    labels = [m['concept'] for m in meta]
    for label in labels:
      if label in taxonomy.content:
        labels_count['content'][label] = labels_count['content'][label] + 1 if label in labels_count['content'] else 1
      else:
        labels_count['category'][label] = labels_count['category'][label] + 1 if label in labels_count['category'] else 1

    # Change format to {user: [list of labels]}
    user_labels = {}
    for m in meta:
      if m['userId'] in user_labels:
        user_labels[m['userId']].append(m['concept'])
      else:
        user_labels[m['userId']] = [m['concept']]

    # Compute number of content duplicates
    for _, labels in user_labels.items():
      count = sum([1 for l in labels if l in taxonomy.content])
      duplicates_count['content'] += count-1 if count else 0

    # Compute number of category duplicates
    for _, label in user_labels.items():
      counts = []
      for category in taxonomy.categories:
        counts.append(sum([1 for l in labels if l in category.positive]))
      duplicates_count['category'] += sum(counts) - sum(c > 0 for c in counts)

  # Total number of content labels and category labels 
  labels_total = {'content': sum([v for k, v in labels_count['content'].items()]),
                  'category': sum([v for k, v in labels_count['category'].items()])}

  # Percentage distribution of labels
  content_distr, category_distr = {}, {}
  for k, v in labels_count['content'].items():
    content_distr[k] = (v*100.0)/labels_total['content']
  for k, v in labels_count['category'].items():
    category_distr[k] = (v*100.0)/labels_total['category']
  labels_distr = {'content': content_distr, 'category': category_distr}    

  logger.info("Distributiuons computed.")
  return labels_count, labels_distr, labels_total, duplicates_count

    
def plot_results(taxonomy, input_count, not_annotated_count, no_consensus_count, totals,
                 labels_count, labels_distr, labels_total, duplicates_count, conflict_count):
    ''' Print results in the console '''
    
    print("\n**************************************************\n")
    print("Retrieved: {} ".format(input_count))
    print("Not annotated: {} | No consensus: {}".format(not_annotated_count, no_consensus_count))
    print("In conflict: {}".format(conflict_count))
    print("\n--------------------------------------------------")

    # Print total count for every category
    for category in taxonomy.categories:
      print("{} --- Positives: {} | Negatives: {} | Safe: {}".
            format(category.aggr_positive, totals[category.name]['_LP_'], totals[category.name]['_LN_'], totals[category.name]['_LS_']))
    print("--------------------------------------------------\n")

    # Print distribution of content labels
    for label in sorted(labels_distr['content'].keys()):
      print("{:.2f} % ({})\t {}".
            format(labels_distr['content'][label], labels_count['content'][label], label))
    print("--------------------------------------------------")
    print("Total number: {} | Duplicates: {:.2f} % ({})".
          format(labels_total['content'], 
                 (duplicates_count['content']*100)/labels_total['content'], 
                 duplicates_count['content']))

    print("\n")   

    # Print distribution of category labels
    for label in sorted(labels_distr['category'].keys()):
      print("{:.2f} % ({})\t {}".
            format(labels_distr['category'][label], labels_count['category'][label], label))
    print("--------------------------------------------------")
    print("Total number: {} | Duplicates: {:.2f} % ({})".
          format(labels_total['category'], 
                 (duplicates_count['category']*100)/labels_total['category'], 
                 duplicates_count['category']))   

    print("\n**************************************************\n")
    

def get_conflicting_annotations(input_ids, conflict_ids, annotations_meta, consensus):
  ''' Get information about annotations with conflicting consensus'''

  conflicts = {}
  for input_id in conflict_ids:

    # Get initial information
    input = input_ids[input_id]

    # Add information about results
    input['consensus'] = consensus[input_id]
    concepts = [annotation['concept'] for annotation in annotations_meta[input_id]]
    input['annotations'] = {concept:concepts.count(concept) for concept in concepts}
    conflicts[input_id] = input

    # Add raw information about annotations
    input['annotation_meta'] = annotations_meta[input_id]
  
  logger.info("Data about conflicting annotations is extracted and stored.")

  return conflicts


def main(args):

  logger.info("----- Experiment {} running -----".format(args.tag))

  # Get taxonomy for current experiment
  taxonomy = get_taxonomy_object(args.group)

  # Get input ids
  input_ids, input_count = get_input_ids(args)

  # Get annotations for every id together with their aggregations
  annotations, annotations_meta, special_labels = get_annotations(args, taxonomy, input_ids)
  aggregated_annotations, not_annotated_count = aggregate_annotations(input_ids, annotations)

  # Compute consensus
  consensus, no_consensus_count, conflict_ids = compute_consensus(taxonomy, input_ids, aggregated_annotations)

  # Compute results
  classes = assign_classes(taxonomy, input_ids, consensus)
  totals = compute_totals(taxonomy, input_ids, classes)

  # Compute distirbution of different annotations
  labels_count, labels_distr, labels_total, duplicates_count = compute_distribution(taxonomy, annotations_meta)

  # Plot statistics using computed values
  plot_results(taxonomy, input_count, not_annotated_count, no_consensus_count, totals,
               labels_count, labels_distr, labels_total, duplicates_count, len(conflict_ids))

  # Save annotations in csv file
  save_labels_csv(args, input_ids, classes, 'labels', special_labels)

  if conflict_ids:
    conflicts = get_conflicting_annotations(input_ids, conflict_ids, annotations_meta, consensus)
    utils.save_data(args.save_conflicts, args.out_path, conflicts, args.tag, 'conflicts')
  else:
    logger.info("No conflicts in annotations. Nothing to dump.")


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Run tracking.")
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.") 
  parser.add_argument('--group',
                      default='Hate_Speech',
                      choices={'Hate_Speech', 'Group_1', 'Group_2'}, # Group_1: adults and drugs. Group_2: crime and OP
                      help="Name of the group.")  
  parser.add_argument('--tag',
                      default='',
                      help="Name of the process/application.")                                
  parser.add_argument('--out_path', 
                      default='', 
                      help="Path to general output directory for this script.")
  parser.add_argument('--save_labels',
                      default=True,
                      type=lambda x: (str(x).lower() == 'true'),
                      help="Save labels in csv file or not.")
  parser.add_argument('--save_conflicts',
                      default=False,
                      type=lambda x: (str(x).lower() == 'true'),
                      help="Save information about annotations with conflicting consensus.")

  args = parser.parse_args()
  args.metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args)