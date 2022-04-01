import pandas as pd
import csv
import os
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
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict


# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)

MAX_ANNOT_PER_VIDEO = 25
REQ_PER_PAGE = 1000
CHUNK_SIZE = REQ_PER_PAGE // MAX_ANNOT_PER_VIDEO

LANGUAGE = ''
OUT_PATH = ''
GROUPS = ['Hate_Speech', 'Adult_Drugs', 'Crime_Obscenity', 'Death_Terrorism_Arms', 'Piracy_DSSI_Spam']

API_KEYS = [] # %INSERT API KEYS HERE%

def get_ids(api_key):

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
        for input in list_inputs_response.inputs:
            meta = MessageToDict(input)['data']['metadata']
            video_id = meta['video_id'] if 'video_id' in meta else meta['id']

            if video_id in ids.values():
                raise Exception(f"Duplicated video id ({video_id}) corresponding to input id: {input.id} in app: {api_key}") 

            ids[input.id] = video_id
            # if input.status.code == status_code_pb2.INPUT_DOWNLOAD_SUCCESS:
            #     ids[input.id] = video_id
            # else:
            #     print (f" *********  BAD VIDEO:  video id: {video_id} corresponding to input id: {input.id} in app: {api_key}") 

    # # ------ DEBUG CODE
    # ids_ = {}
    # for id in list(ids.keys())[0:5]:
    #   ids_[id] = ids[id]
    # ids = ids_
    # # ------ DEBUG CODE
    
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
        labels[id] = input_labels_

    return labels


def export(gt, final_labels):
    df = pd.DataFrame()
    df['video_id'] = gt.keys()
    df['ground_truth'] = gt.values()
    df['human_labels'] = final_labels.values()
    df.to_csv(OUT_PATH, index=False)

# ----------------- MAIN

# Get video ids common btw gt and annotated data
logger.info(f" ------ Extracting ids...") 
ids = []
for i, group in enumerate(API_KEYS):
    logger.info(f"Group: {GROUPS[i]}") 
    group_ids = {}
    for batch_num, api_key in group.items():
        logger.info(f"Batch: {batch_num}") 
        group_ids[batch_num] = get_ids(api_key)
    ids.append(group_ids)
logger.info(f"Video ids extracted.") 
pickle.dump(ids, open(os.path.join(OUT_PATH, f'{LANGUAGE}_ids.pickle'), "wb"))
# ids = pickle.load(open(os.path.join(OUT_PATH, f'{LANGUAGE}_ids.pickle'), "rb"))

# Get raw (ungrouped) annotations
logger.info(f" ------ Extracting raw annotations...") 
raw_annotations = []
for i, group in enumerate(API_KEYS):
    logger.info(f"Group: {GROUPS[i]}") 
    group_annotations = {}
    for batch_num, api_key in group.items():
        logger.info(f"Batch: {batch_num}") 
        batch_ids = ids[i][batch_num]
        batch_annotations = get_raw_annotations(api_key, batch_ids)
        group_annotations[batch_num] = batch_annotations
        # Pickle
        # pickle.dump(batch_annotations, open(os.path.join(OUT_PATH, f'{LANGUAGE}_{GROUPS[i]}_batch_{batch_num}.pickle'), "wb"))
    raw_annotations.append(group_annotations)
logger.info(f"Raw annotations extracted.")

# Get processed labels (consensus)
logger.info(f" ------ Transforming annotations into labels...") 
labels = []
for i, group in enumerate(API_KEYS):
    group_labels = {}
    group_taxonomy = get_taxonomy_object(GROUPS[i])
    for batch_num in group.keys():
        batch_ids = ids[i][batch_num]
        group_labels[batch_num] = get_labels(raw_annotations[i][batch_num], group_taxonomy)
    labels.append(group_labels)
logger.info(f"Labels obtained.")

# Connect all lables 
final_labels = {}
for i, group_labels in enumerate(labels):
    for batch_num, batch_labels in group_labels.items():
        for input_id, video_labels in batch_labels.items():
            video_id = ids[i][batch_num][input_id]
            if video_id in final_labels:
                final_labels[video_id].extend(video_labels)
            else:
                final_labels[video_id] = video_labels
pickle.dump(final_labels, open(os.path.join(OUT_PATH, f'{LANGUAGE}_human_labels.pickle'), "wb"))

# Save file labels to csv file
with open(os.path.join(OUT_PATH, f'{LANGUAGE}_human_labels.csv'), 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['video_id', 'labels'])
    for video_id, video_labels in final_labels.items():
        writer.writerow([video_id, video_labels])

