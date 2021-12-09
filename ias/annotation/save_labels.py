import os
import csv
import logging
import taxonomy

def save_labels_csv(args, input_ids, classes, name):
    ''' Dump labels to a csv file '''
    
    if args.save_labels:
        # Create output dir if needed
        path = os.path.join(args.out_path, name)
        if not os.path.exists(path):
            os.mkdir(path)

        # Set file path
        file_path = os.path.join(path, "{}_{}.csv".format(args.tag, name))

        # Write to file
        with open(file_path, 'w', encoding='UTF8', newline='') as f:

            writer = csv.writer(f)
            header = ['input_id', 'video_id', 'video_description', 'video_url'] + \
                      list(taxonomy.CATEGORY_IDX.keys())
            writer.writerow(header)

            # Dump labels for every input
            for input_id in input_ids:
                row = _from_input_to_output(input_id, input_ids[input_id], classes[input_id])
                writer.writerow(row)

        logging.info("Annotation saved to {}".format(file_path))


def add_final_labels_to_metadata(input_ids, classes):
    ''' Add labels to inputs metadata '''    

    for input_id in input_ids:
        input_ids[input_id]['final_labels'] = _from_classes_to_meta_labels(classes[input_id])
    
    logging.info("Final labels added to inputs metadata.")
    return input_ids


def _from_input_to_output(input_id, input, classes):
    ''' Make one output row from one input and its labels'''
    
    meta = [input_id, input['video_id'], input['description'], input['url']]
    labels = _from_classes_to_labels(classes)
    return meta + labels


def _from_classes_to_labels(classes):
    ''' Transforms a dictionary of {positive category: class} to a list of labels to save in output file'''
    
    labels = ['n/a'] * len(taxonomy.CATEGORY_IDX) # not in the experiment or not annotated

    if classes is not None:
        for label_, class_ in classes.items():
            if class_ == '_LP_':
                labels[taxonomy.CATEGORY_IDX[label_]] = '1' # positive
            elif class_ == '_LS_':
                labels[taxonomy.CATEGORY_IDX[label_]] = '0' # safe
            elif class_ == '_LN_':
                labels[taxonomy.CATEGORY_IDX[label_]] = '-1' # negative but not safe

    return labels


def _from_classes_to_meta_labels(classes):
    ''' Transforms a dictionary of {positive label: class} to a list of labels to save in output file'''
    
    labels = ['n/a'] * len(taxonomy.CATEGORY_IDX) # not in the experiment or not annotated

    if classes is not None:
        for label_, class_ in classes.items():
            if class_ == 'P':
                labels[taxonomy.CATEGORY_IDX[label_]] = '1' # positive
            elif class_ == 'S':
                labels[taxonomy.CATEGORY_IDX[label_]] = '0' # safe
            elif class_ == 'N':
                labels[taxonomy.CATEGORY_IDX[label_]] = '-1' # negative but not safe
    
    final_labels = {}
    for category, idx in taxonomy.CATEGORY_IDX.items():
        final_labels[category] = labels[idx]

    return final_labels
