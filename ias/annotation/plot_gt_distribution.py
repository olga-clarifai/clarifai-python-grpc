import json
import csv
import matplotlib.pyplot as plt

import ground_truth as gt

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


ENG_path = ''
DE_path = ''
FR_path = ''

ENG_API_KEY = ''
DE_API_KEY = ''
FR_API_KEY = ''

CATEGORIES = {'Adult': 'adult_&_explicit_sexual_content',
              'Arms': 'arms_&_ammunition',
              'Crime': 'crime',
              'Death': 'death,_injury_or_military_conflict',
              'Hate speech': 'hate_speech',
              'Drugs': 'illegal_drugs/tobacco/e-cigarettes/vaping/alcohol',
              'Obscenity': 'obscenity_&_profanity',
              'Piracy': 'online_piracy',        
              'Social issue': 'debated_sensitive_social_issue',      
              'Spam': 'spam_or_harmful_content',
              'Terrorism': 'terrorism',
              'Not GARM': 'safe'
}

CATEGORY_ABBR = {value: key for key, value in CATEGORIES.items()}


# def extract_gt_app(path, api_key):

#     metadata = (('authorization', 'Key {}'.format(api_key)),)
#     labels = {c: 0 for c in CATEGORIES.keys()}

#     # Get inputs
#     for page in range(1,5):
#         list_inputs_response = stub.ListInputs(
#                             service_pb2.ListInputsRequest(page=page, per_page=1000),
#                             metadata=metadata
#         )

#         # Process ground turth
#         for input in list_inputs_response.inputs:
#             input = MessageToDict(input)
#             ground_truth = input['data']['metadata']['ground_truth']
#             for label in ground_truth:
#                 labels[CATEGORY_ABBR[label]] += 1

#     return labels

def extract_gt_app(path, api_key):

    metadata = (('authorization', 'Key {}'.format(api_key)),)
    labels = {c: 0 for c in CATEGORIES.keys()}

    video_ids = []
    for page in range(1,5):
        list_inputs_response = stub.ListInputs(
                            service_pb2.ListInputsRequest(page=page, per_page=1000),
                            metadata=metadata
        )
        for input in list_inputs_response.inputs:
            input = MessageToDict(input)
            if 'video_id' in input['data']['metadata']:
                video_ids.append(input['data']['metadata']['video_id'])
            elif 'id' in input['data']['metadata']:
                video_ids.append(input['data']['metadata']['id'])

    ground_truth = gt.load_all_from_csv(path, 'safe')

    for video_id in ground_truth:
        if video_id in video_ids:
            for label in ground_truth[video_id]:
                labels[CATEGORY_ABBR[label]] += 1

    return labels


def extract_gt_labels_csv(path):

    # Load ground truth if available
    ground_truth = gt.load_all_from_csv(path, 'safe')

    labels = {c: 0 for c in CATEGORIES.keys()}
    for id, video_labels in ground_truth.items():
        for label in video_labels:
            labels[CATEGORY_ABBR[label]] += 1

    return labels


def extract_gt_labels_json(path):

    # read file
    f = open (path, "r")
    inputs = json.loads(f.read())

    labels = {c: 0 for c in CATEGORIES.keys()}
    for id, meta in inputs.items():
        for label in meta['ground_truth']:
            labels[CATEGORY_ABBR[label]] += 1

    return labels


def plot_distribution(ENG_labels, DE_labels, FR_labels):

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(20,5))

    ax1.barh(list(ENG_labels.keys()), list(ENG_labels.values())) 
    ax1.invert_yaxis()
    ax1.set_xticklabels([])
    ax1.set_xticks([])
    ax1.set_xlim([0, max(ENG_labels.values()) + 30])
    ax1.title.set_text('ENG')
    for i, v in enumerate(ENG_labels.values()):
        ax1.text(v + 3, i + 0.1, str(v))

    ax2.barh(list(DE_labels.keys()), list(DE_labels.values())) 
    ax2.invert_yaxis()
    # ax2.set_yticklabels([])
    # ax2.set_yticks([])
    ax2.set_xticklabels([])
    ax2.set_xticks([])
    ax2.set_xlim([0, max(DE_labels.values()) + 75])
    ax2.title.set_text('DE')
    for i, v in enumerate(DE_labels.values()):
        ax2.text(v + 6, i + 0.1, str(v))

    ax3.barh(list(FR_labels.keys()), list(FR_labels.values())) 
    ax3.invert_yaxis()
    # ax3.set_yticklabels([])
    # ax3.set_yticks([])
    ax3.set_xticklabels([])
    ax3.set_xticks([])
    ax3.set_xlim([0, max(FR_labels.values()) + 15])
    ax3.title.set_text('FR')
    for i, v in enumerate(FR_labels.values()):
        ax3.text(v + 1, i + 0.1, str(v))

    plt.subplots_adjust(wspace = 6)
    fig.tight_layout()
    plt.show()  


# ----------- MAIN ---------- #
# ENG_labels = extract_gt_app(ENG_path, ENG_API_KEY)
# DE_labels = extract_gt_app(DE_path, DE_API_KEY)
# FR_labels = extract_gt_app(FR_path, FR_API_KEY)

ENG_labels = extract_gt_labels_csv(ENG_path)
DE_labels = extract_gt_labels_csv(DE_path)
FR_labels = extract_gt_labels_csv(FR_path)

# ENG_labels = extract_gt_labels_json(ENG_path)
# DE_labels = extract_gt_labels_json(DE_path)
# FR_labels = extract_gt_labels_json(FR_path)

plot_distribution(ENG_labels, DE_labels, FR_labels)
