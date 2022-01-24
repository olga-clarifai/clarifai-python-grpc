import csv
import logging
from taxonomy import CATEGORIES

GT_LABELS = CATEGORIES.values()

GT_LABELS_ = [label + '_y' for label in GT_LABELS]

GT_LABELS_IDX = {label: idx for label, idx in zip(GT_LABELS, range(len(GT_LABELS )))}


def load_all_from_csv(csv_path, safe_gt_label):
    '''Load ground truth for videos selection/upload. Format {video_id: gt_labels}.'''

    # Extract ground truth labels for every input in the file and link it to video id
    ground_truth = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for line in reader:
            line = {k.lower(): v for k, v in line.items()}

            gt_labels = []
            correct = True

            # Extract labels
            for key in line:
                if _clean_key(key) is not None:
                    # Catch exceptions to avoid errors related to incorrect ground truth entries
                    try:
                        if int(line[key]):
                            gt_labels.append(_clean_key(key))
                    except:
                        logging.warning('\t Incorrect ground truth for {}. Input ignored.'.format(line['video_id']))
                        correct = False
                        break
            if not gt_labels:
                gt_labels = [safe_gt_label]

            # Check if ground truth is all good
            if correct:
                ground_truth[line['video_id']] = gt_labels

    logging.info("Ground truth was extracted from csv for {} inputs.".format(len(ground_truth)))

    return ground_truth


def load_from_csv(input_ids, csv_path, safe_gt_label):
    '''Load ground truth for pilots evaluation. Format {input_id: gt_labels}.'''

    # Map video ids to their input id
    video_to_input = {meta['video_id']: id for id, meta in input_ids.items()}

    # Extract ground truth labels for every video present in the file
    ground_truth = {} 
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for line in reader:
            line = {k.lower(): v for k, v in line.items()}

            # If exists in ground truth file, extract ground truth labels
            if line['video_id'] in video_to_input.keys():
                gt_labels = []
                for key in line:
                    if _clean_key(key) is not None:
                        # Catch exceptions to avoid errors related to incorrect ground truth entries
                        try:
                            if int(line[key]):
                                gt_labels.append(_clean_key(key))
                        except:
                            logging.warning('\t Incorrect ground truth for {}. Input ignored.'.format(line['video_id']))
                            break
                if not gt_labels:
                    gt_labels = [safe_gt_label]
                ground_truth[video_to_input[line['video_id']]] = gt_labels
    
    # Count number of inputs with no ground truth
    no_gt_count = sum([1 for input_id in input_ids if not input_id in ground_truth]) 
    
    if no_gt_count > 0:
        logging.info("Ground truth was extracted from csv. {} inputs do not have ground truth.". format(no_gt_count)) 
    else:
        logging.info("Ground truth was extracted from csv for all inputs.")

    return ground_truth, no_gt_count
        

def get_from_meta(meta):

    labels = [0] * len(GT_LABELS)
    for label in GT_LABELS:
        if label in meta:
            labels[GT_LABELS_IDX[label]] = 1
    return labels


def _clean_key(key):
    
    if key in GT_LABELS:
        return key
    elif key in GT_LABELS_:
        return key[:-2]
    else:
        return None