import pandas as pd
import csv
from taxonomy import CATEGORIES, SPECIAL_USE_LABELS, get_taxonomy_object
from ground_truth import GT_LABELS, GT_LABELS_, GT_LABELS_IDX, _clean_key
import utils
from tqdm import tqdm
from collections import Counter
import itertools
import pickle
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score

from sklearn.preprocessing import MultiLabelBinarizer

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import service_pb2, service_pb2_grpc
from google.protobuf.json_format import MessageToDict


# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)

MAX_ANNOT_PER_VIDEO = 25
REQ_PER_PAGE = 1000
CHUNK_SIZE = REQ_PER_PAGE // MAX_ANNOT_PER_VIDEO

CATEGORIES_INV = {v: k for k, v in CATEGORIES.items()}

GROUND_TRUTH = ''
OUT_PATH = ''

GROUPS = ['Hate_Speech', 'Adult_Drugs', 'Crime_Obscenity', 'Death_Terrorism_Arms', 'Piracy_DSSI_Spam']
UNVAILABLE_LABELS = ['piracy', 'social', 'spam']

BATCHES = [] # %INSERT API KEYS HERE%


def load_ground_truth():

    ground_truth = {} # key - video_id, value - list of labels
    corrupted_gt = 0

    # Extract ground truth labels for every video present in the file
    with open(GROUND_TRUTH, 'r') as f:
        reader = csv.DictReader(f)
        for line in reader:
            line = {k.lower(): v for k, v in line.items()}

            # If exists in ground truth file, extract ground truth labels
            gt_labels = []
            for key in line:
                if _clean_key(key) is not None:
                    # Catch exceptions to avoid errors related to incorrect ground truth entries
                    try:
                        if int(line[key]):
                            gt_labels.append(CATEGORIES_INV[_clean_key(key)])
                    except:
                        corrupted_gt += 1
                        break
            ground_truth[line['video_id']] = gt_labels
    
    logger.info(f"Ground truth was extracted from csv. {corrupted_gt} inputs do not have ground truth.") 
    return ground_truth


def get_input_ids(api_key, gt):

    metadata = (('authorization', 'Key {}'.format(api_key)),)
    ids = {} # key - input_id, value - video_id

    # Get inputs
    for page in range(1,13):
        list_inputs_response = stub.ListInputs(
                            service_pb2.ListInputsRequest(page=page, per_page=1000),
                            metadata=metadata
        )
        utils.process_response(list_inputs_response)

        # Process inputs in response
        for input_object in list_inputs_response.inputs:
            meta = MessageToDict(input_object)['data']['metadata']
            video_id = meta['video_id'] if 'video_id' in meta else meta['id']
            if video_id in gt.keys():
                ids[input_object.id] = video_id
    
    return ids


def extract_concept(annotation):

    annotation = MessageToDict(annotation)

    # Regular concept
    if 'concepts' in annotation['data']:
        if len(annotation['data']['concepts']) > 0:
            return annotation['data']['concepts'][0]['name']

    # Time segment
    elif 'timeSegments' in annotation['data']:
        if 'concepts' in annotation['data']['timeSegments'][0]['data']:
            if len(annotation['data']['timeSegments'][0]['data']) > 0:
                return annotation['data']['timeSegments'][0]['data']['concepts'][0]['name']

    return None


def get_raw_annotations(api_key, ids):

    metadata = (('authorization', 'Key {}'.format(api_key)),)

    annotations = {input_id: {} for input_id in ids.keys()}

    # Split inputs into chuncks and make a separate request for each chunk
    chunks = [list(ids.keys())[i:i + CHUNK_SIZE] for i in range(0, len(ids.keys()), CHUNK_SIZE)]
    for chunk in tqdm(chunks, total=len(chunks)):

        # Make and process request
        list_annotations_response = stub.ListAnnotations(
                                        service_pb2.ListAnnotationsRequest(
                                            input_ids=chunk, 
                                            per_page=REQ_PER_PAGE,
                                            list_all_annotations=True
                                    ),
        metadata=metadata
        )

        # Loop through all annotations
        for annotation in list_annotations_response.annotations:

            # Get info
            input_id = annotation.input_id
            user_id = annotation.user_id
            
            # Extract and save concept
            if user_id != 'ias':
                concept = extract_concept(annotation)
                if concept is not None and concept not in SPECIAL_USE_LABELS:
                    if user_id in annotations[input_id].keys() and concept not in annotations[input_id][user_id]:
                        annotations[input_id][user_id].append(concept)
                    else:
                        annotations[input_id][user_id] = [concept]

    # Eliminate user ids
    annotations = {input_id: annotations[input_id].values() for input_id in annotations.keys()}
    annotations = {input_id: list(itertools.chain(*annotations[input_id])) for input_id in annotations}

    return annotations


def get_labels(raw_annotations, taxonomy):

    labels = {}
    for id, annotations in raw_annotations.items():

        # Shorten annotation names and save only positives
        input_labels = []
        for category in taxonomy.categories:
            for annotation in annotations:
                if annotation in category.positive:
                    input_labels.append(category.aggr_positive)
        
        # Compute consensus
        input_labels = dict(Counter(input_labels))
        input_labels_ = []
        for label, count in input_labels.items():
            if count >= 3:
                input_labels_.append(label)
            else:
                input_labels_.append(f'_{label}_')
        labels[id] = input_labels_

    return labels


def hamming_score(gt, fl):
    temp = 0
    for i in range(gt.shape[0]):
        if sum(gt[i]) == 0 and sum(fl[i]) == 0:
            temp += 1
        else:
            temp += sum(np.logical_and(gt[i], fl[i])) / sum(np.logical_or(gt[i], fl[i]))
    return temp / gt.shape[0]

def export(gt, final_labels):
    df = pd.DataFrame()
    df['video_id'] = gt.keys()
    df['ground_truth'] = gt.values()
    df['human_labels'] = final_labels.values()
    df.to_csv(OUT_PATH, index=False)

# ----------------- MAIN

# Load ground truth
gt = load_ground_truth()

# Get video ids common btw gt and annotated data
ids = []
for group in BATCHES:
    group_ids = {}
    for batch_num, api_key in group.items():
        group_ids[batch_num] = get_input_ids(api_key, gt)
    ids.append(group_ids)
logger.info(f"Common video ids extracted.") 

# Get dict of all ids
ids_dict = {}
for i, group in enumerate(BATCHES):
    for batch_num in group:
        ids_ = ids[i][batch_num]
        ids_dict = {**ids_dict, **ids_}

# Get from gt only common ids (sort them) and eliminate unavailable groups
gt = {video_id: labels for video_id, labels in gt.items() if video_id in ids_dict.values()}
gt = {video_id: gt[video_id] for video_id in ids_dict.values()}
gt_ = {}
for video_id in ids_dict.values():
    gt_[video_id] = [label for label in gt[video_id] if label not in UNVAILABLE_LABELS]

# Get raw (ungrouped) annotations
raw_annotations = []
for i, group in enumerate(BATCHES):
    group_annotations = {}
    for batch_num, api_key in group.items():
        batch_ids = ids[i][batch_num]
        group_annotations[batch_num] = get_raw_annotations(api_key, batch_ids)
    raw_annotations.append(group_annotations)
logger.info(f"Raw annotations extracted.")

# Get processed labels (consensus)
labels = []
for i, group in enumerate(BATCHES):
    group_labels = {}
    group_taxonomy = get_taxonomy_object(GROUPS[i])
    for batch_num, api_key in group.items():
        batch_ids = ids[i][batch_num]
        group_labels[batch_num] = get_labels(raw_annotations[i][batch_num], group_taxonomy)
    labels.append(group_labels)

# Connect all lables 
final_labels = {}
for i, group_labels in enumerate(labels):
    for batch_num, batch_labels in group_labels.items():
        for input_id, labels in batch_labels.items():
            video_id = ids[i][batch_num][input_id]
            if video_id in final_labels:
                final_labels[video_id].extend(labels)
            else:
                final_labels[video_id] = labels
final_labels = {video_id: final_labels[video_id] for video_id in ids_dict.values()}

# Excluse unvailable labels
gt_ = {}
final_labels_ = {}
for input_id, labels in gt.items():
    labels = [label for label in labels if label not in UNVAILABLE_LABELS]
    if labels:
        gt_[input_id] = labels
        final_labels_[input_id] = final_labels[input_id]

# Compute for consensus only
fl = final_labels_
classes = list(CATEGORIES.keys()) + [f'_{key}_' for key in CATEGORIES.keys()]
mlb = MultiLabelBinarizer(classes=classes)
gt_bi = mlb.fit_transform(gt_.values())
fl_bi = mlb.fit_transform(fl.values())
hamming_consensus = hamming_score(gt_bi, fl_bi)
accuracy = accuracy_score(gt_bi, fl_bi)

# Compute for all (even with no consensus)
fl = {}
for id, labels in final_labels_.items():
    fl[id] = [label.strip('_') for label in labels]
mlb = MultiLabelBinarizer(classes=list(CATEGORIES.keys()))
gt_bi = mlb.fit_transform(gt_.values())
fl_bi = mlb.fit_transform(fl.values())
hamming_all = hamming_score(gt_bi, fl_bi)

# Save all labels into a file
export(gt, final_labels)

logger.info(f'Number of gt inputs in annotated data: {sum([len(batch.keys()) for batch in ids[0].values()])}')
logger.info(f'Number of available inputs: {len(final_labels_)}')
logger.info(f'Accuracy: {accuracy} | Humming consensus: {hamming_consensus} | Humming all: {hamming_all}')
