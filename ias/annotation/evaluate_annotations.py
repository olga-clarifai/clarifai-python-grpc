import os
import argparse
import utils
from tqdm import tqdm

import ground_truth as gt
from taxonomy import get_taxonomy_object, CATEGORIES, SPECIAL_USE_LABELS, GT_SAFE_LABEL

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict

# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def get_input_ids(metadata):
  ''' Get list of all inputs (ids of videos that were uploaded) from the app '''

  # Get inputs
  list_inputs_response = stub.ListInputs(
                         service_pb2.ListInputsRequest(page=1, per_page=1000),
                         metadata=metadata
  )
  utils.process_response(list_inputs_response)

  # Extract input ids
  input_ids = {}
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
  # for id in list(input_ids.keys())[0:50]:
  #   input_ids_[id] = input_ids[id]
  # input_ids = input_ids_
  # print("Number of selected inputs: {}".format(len(input_ids)))
  # # ------ DEBUG CODE

  return input_ids, len(input_ids)


def get_ground_truth(args, input_ids):
  ''' Get list of ground truth concepts for every input id'''

  assert os.path.exists(args.ground_truth), f"Ground truth file {args.ground_truth} doesn't exist"
  ground_truth, no_gt_count = gt.load_from_csv(input_ids, args.ground_truth, GT_SAFE_LABEL)

  # # ------ DEBUG CODE
  # # Compute list of unique labels
  # labels = list(itertools.chain(*[ground_truth[input_id] for input_id in input_ids]))
  # labels_count = {l:labels.count(l) for l in labels}
  # print("Ground truth labels are: ")
  # [print("\t{}: {}".format(k, v)) for k, v in labels_count.items()]
  # # ------ DEBUG CODE

  return ground_truth, no_gt_count


def remove_inputs_without_gt(input_ids, ground_truth):
  ''' Eliminate inputs that do not have any ground truth'''

  input_ids_with_gt = {}
  for input_id in input_ids:
    if input_id in ground_truth:
      input_ids_with_gt[input_id] = input_ids[input_id]

  return input_ids_with_gt


def get_annotations(args, taxonomy, input_ids):
  ''' Get list of annotations for every input id'''

  logger.info("Fetching annotations...")

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

      # Check for concepts in data but also time segments
      concepts = []
      if 'concepts' in ao['data'] and len(ao['data']['concepts'])>0:
        concepts.append(ao['data']['concepts'][0]['name'])
      elif 'timeSegments' in ao['data'] and 'concepts' in ao['data']['timeSegments'][0]['data'] and \
           len(ao['data']['timeSegments'][0]['data'])>0:
        concepts.append(ao['data']['timeSegments'][0]['data']['concepts'][0]['name'])

      # Get meta about all found concepts
      for concept in concepts:
        meta_.append((concept, ao['userId']))

    # Remove duplicates from meta, transform it into dictionary (for further convenience) and store
    meta = set(meta_)
    meta = [{'concept': m[0], 'userId': m[1]} for m in meta]
    annotations_meta[input_id] = meta

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
          elif annotation in category.safe or annotation == '2-none-of-the-above':
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

  logger.info("Annotations fetched")
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


def compute_classes(args, taxonomy, input_ids, consensus, ground_truth):
  ''' Compute belonging of each input to the following classes: 
      Ground truth _GP_ (positive), _GN_ (negative), _GS_ (safe), and
      Labels _LP_ (positive), _LN_ (negative), _LS_ (safe) '''

  classes = {}
  for input_id in input_ids:
    classes_ = []

    if input_id in consensus:
      # Human annotations
      if consensus[input_id] is None:
        classes_.append('_LN_')
      else:
        if taxonomy.categories[0].aggr_positive in consensus[input_id]:
          classes_.append('_LP_')
        elif taxonomy.categories[0].aggr_safe in consensus[input_id]:
          classes_.append('_LN_')
          classes_.append('_LS_')
        else:
          classes_.append('_LN_')

      # Ground truth first
      if CATEGORIES[args.category] in ground_truth[input_id]:
        classes_.append('_GP_')
      elif GT_SAFE_LABEL in ground_truth[input_id]:
        classes_.append('_GN_')
        classes_.append('_GS_')
      else:
        classes_.append('_GN_')

      classes[input_id] = classes_
    else:
      classes[input_id] = None

  logger.info("Classes computed.")
  return classes


def compute_totals(input_ids, classes):
  ''' Compute total number of inputs for each class '''

  totals = {'_GP_': 0, '_GN_': 0, '_GS_': 0, '_LP_': 0, '_LN_': 0, '_LS_': 0}

  for input_id in input_ids:
    if input_id in classes and classes[input_id] is not None:
      if '_GP_' in classes[input_id]:
        totals['_GP_'] += 1
      if '_GN_' in classes[input_id]:
        totals['_GN_'] += 1
      if '_GS_' in classes[input_id]:
        totals['_GS_'] += 1
      if '_LP_' in classes[input_id]:
        totals['_LP_'] += 1
      if '_LN_' in classes[input_id]:
        totals['_LN_'] += 1
      if '_LS_' in classes[input_id]:
        totals['_LS_'] += 1

  logger.info("Totals computed.")
  return totals
  

def compute_metrics(input_ids, classes, totals):
  ''' Compute defined matrics '''

  markers = {}

  # Compute counts
  metric_counts = {'TP': 0, 'TN': 0, 'STN': 0, 'FP': 0, 'SFP': 0, 'FN': 0, 'SFN': 0}
  for input_id in input_ids: 
    if input_id in classes:
      # True Positive (positive annotation and positive ground truth)
      if '_LP_' in classes[input_id] and '_GP_' in classes[input_id]:
        metric_counts['TP'] += 1
        markers[input_id] = 'TP'
      # True Negative (negative annotation and negative ground truth)
      if '_LN_' in classes[input_id] and '_GN_' in classes[input_id]:
        metric_counts['TN'] += 1
        markers[input_id] = 'TN'
      # Safe True Negative (safe annotation and safe ground truth)
      if '_LS_' in classes[input_id] and '_GS_' in classes[input_id]:
        metric_counts['STN'] += 1
        markers[input_id] = 'STN'
      # False Positive (positive annotation and negative ground truth)
      if '_LP_' in classes[input_id] and '_GN_' in classes[input_id]:
        metric_counts['FP'] += 1
        markers[input_id] = 'FP'
      # Safe False Positive (positive annotation and safe ground truth)
      if '_LP_' in classes[input_id] and '_GS_' in classes[input_id]:
        metric_counts['SFP'] += 1
        markers[input_id] = 'SFP'
      # False Negative (negative annotation and positive ground truth)
      if '_LN_' in classes[input_id] and '_GP_' in classes[input_id]:
        metric_counts['FN'] += 1
        markers[input_id] = 'FN'
      # Safe False Safe (safe annotation and positive ground truth)
      if '_LS_' in classes[input_id] and '_GP_' in classes[input_id]:
        metric_counts['SFN'] += 1
        markers[input_id] = 'SFN'
    
  # Compute rates
  metric_rates = {}
  metric_rates['TP'] = metric_counts['TP'] / totals['_GP_'] if metric_counts['TP'] != 0 else 0
  metric_rates['TN'] = metric_counts['TN'] / totals['_GN_'] if metric_counts['TN'] != 0 else 0
  metric_rates['STN'] = metric_counts['STN'] / totals['_GS_'] if metric_counts['STN'] != 0 else 0
  metric_rates['FP'] = metric_counts['FP'] / totals['_GN_'] if metric_counts['FP'] != 0 else 0
  metric_rates['SFP'] = metric_counts['SFP'] / totals['_GS_'] if metric_counts['SFP'] != 0 else 0
  metric_rates['FN'] = metric_counts['FN'] / totals['_GP_'] if metric_counts['FN'] != 0 else 0
  metric_rates['SFN'] = metric_counts['SFN'] / totals['_GP_'] if metric_counts['SFN'] != 0 else 0

  logger.info("Metrics computed.")
  return metric_counts, metric_rates, markers


def plot_results(input_count, no_gt_count, not_annotated_count, no_consensus_count,
                 metric_counts, metric_rates, totals):
    ''' Print results in the console '''
    
    print("\n*******************************************")
    print("--------------- Ground truth --------------\n")
    print("Not available: {}".format(no_gt_count))
    print("Positives: {} | Negatives: {} | Safe: {}".format(totals['_GP_'], totals['_GN_'], totals['_GS_']))
    print("\n------------------ Labels -----------------\n")
    print("Retrieved: {} | Kept: {}".format(input_count, input_count-no_gt_count))
    print("Not annotated: {} | No consensus: {}".format(not_annotated_count, no_consensus_count))
    print("Positives: {} | Negatives: {} | Safe: {}".format(totals['_LP_'], totals['_LN_'], totals['_LS_']))
    print("\n-------------- Metrics (rates) ------------\n")
    print("TP: \t{}/{}\t\t= {:.2f}".format(metric_counts['TP'], totals['_GP_'], metric_rates['TP']))
    print("TN: \t{}/{}\t\t= {:.2f}".format(metric_counts['TN'], totals['_GN_'], metric_rates['TN']))
    print("STN: \t{}/{}\t\t= {:.2f}".format(metric_counts['STN'], totals['_GS_'], metric_rates['STN']))
    print("FP: \t{}/{}\t\t= {:.2f}".format(metric_counts['FP'], totals['_GN_'], metric_rates['FP']))
    print("SFP: \t{}/{}\t\t= {:.2f}".format(metric_counts['SFP'], totals['_GS_'], metric_rates['SFP']))
    print("FN: \t{}/{}\t\t= {:.2f}".format(metric_counts['FN'], totals['_GP_'], metric_rates['FN']))
    print("SFN: \t{}/{}\t\t= {:.2f}".format(metric_counts['SFN'], totals['_GP_'], metric_rates['SFN']))
    print("*******************************************\n")
    

def get_false_annotations(input_ids, ground_truth, annotations_meta, consensus, markers):
  ''' Get information about inputs that were mislabelled '''

  false_annotations = {}
  for input_id in input_ids:
    if input_id in markers:
      if markers[input_id] == 'FP' or markers[input_id] == 'SFP' or \
         markers[input_id] == 'FN' or markers[input_id] == 'SFN':
        # Get initial information minus some fields
        input = input_ids[input_id]

        # Add information about results
        input['marker'] = markers[input_id]
        input['ground_truth'] = ground_truth[input_id]
        input['consensus'] = consensus[input_id]

        # Count different concepts
        concepts = [annotation['concept'] for annotation in annotations_meta[input_id]]
        input['annotations'] = {concept:concepts.count(concept) for concept in concepts}
        false_annotations[input_id] = input

        # Add raw information about annotations
        input['annotation_meta'] = annotations_meta[input_id]
  
  logger.info("False annotations extracted and stored.")

  return false_annotations
        

def get_conflicting_annotations(input_ids, conflict_ids, ground_truth, annotations_meta, consensus):
  ''' Get information about annotations with conflicting consensus'''

  conflicts = {}
  for input_id in conflict_ids:

    # Get initial information minus some fields
    input = input_ids[input_id]

    # Add information about results
    input['ground_truth'] = ground_truth[input_id]
    input['consensus'] = consensus[input_id]

    # Count different concepts
    concepts = [annotation['concept'] for annotation in annotations_meta[input_id]]
    input['annotations'] = {concept:concepts.count(concept) for concept in concepts}
    conflicts[input_id] = input

    # Add raw information about annotations
    input['annotation_meta'] = annotations_meta[input_id]
  
  logger.info("Data about conflicting annotations is extracted and stored.")

  return conflicts


def main(args):

  logger.info("---------- Pilot {} ----------".format(args.tag))

  # Get taxonomy for current experiment
  taxonomy = get_taxonomy_object(args.category)

  # Get input ids
  input_ids, input_count = get_input_ids(args.metadata)

  # Get ground truth labels for every input and eliminate those that do not have it
  ground_truth, no_gt_count = get_ground_truth(args, input_ids)
  input_ids = remove_inputs_without_gt(input_ids, ground_truth)

  # Get annotations for every id together with their aggregations
  annotations, annotations_meta = get_annotations(args, taxonomy, input_ids)
  aggregated_annotations, not_annotated_count = aggregate_annotations(args, input_ids, annotations)

  # Compute consensus
  consensus, no_consensus_count, conflict_ids = compute_consensus(taxonomy, input_ids, aggregated_annotations)

  # Compute results
  classes = compute_classes(args, taxonomy, input_ids, consensus, ground_truth)
  totals = compute_totals(input_ids, classes)
  metric_counts, metric_rates, markers = compute_metrics(input_ids, classes, totals)

  # Plot statistics using computed values
  plot_results(input_count, no_gt_count, not_annotated_count, no_consensus_count,
               metric_counts, metric_rates, totals) 

  # Get and save fails
  false_annotations = get_false_annotations(input_ids, ground_truth, annotations_meta, consensus, markers)
  utils.save_data(args.save_false_annotations, args.out_path, false_annotations, args.tag, 'false_annotations')
  if conflict_ids:
    conflicts = get_conflicting_annotations(input_ids, conflict_ids, ground_truth, annotations_meta, consensus)
    utils.save_data(args.save_conflicts, args.out_path, conflicts, args.tag, 'conflicts')
  else:
    logger.info("No conflicts in annotations. Nothing to dump.")


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Evaluate annotations.")
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.")                     
  parser.add_argument('--category', 
                      default='obscenity', 
                      choices={'adult', 'crime', 'hate', 'drugs', 'obscenity'},
                      help="Name of the group.")
  parser.add_argument('--tag',
                      default='TEST',
                      help="Name of the process/application.") 
  parser.add_argument('--language',
                      default='',
                      help="Abbreviation of experiment language.")
  parser.add_argument('--ground_truth', 
                      default='', 
                      help="Path to csv file with ground truth.")                    
  parser.add_argument('--out_path', 
                      default='', 
                      help="Path to general output directory for this script.")
  parser.add_argument('--save_false_annotations',
                      default=False,
                      type=lambda x: (str(x).lower() == 'true'),
                      help="Save information about false annotations inputs in file or not.")
  parser.add_argument('--save_conflicts',
                      default=False,
                      type=lambda x: (str(x).lower() == 'true'),
                      help="Save information about annotations with conflicting consensus.")

  args = parser.parse_args()

  args.metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args)