import csv
import logging


LABELS = {'2-AD': 'adult_&_explicit_sexual_content',
          '2-AA': 'arms_&_ammunition',
          '2-CR': 'crime',
          '2-DM': 'death,_injury_or_military_conflict',
          '2-OP': 'online_piracy',
          '2-HB': 'hate_speech',
          '2-OP': 'obscenity_&_profanity',
          '2-ID': 'illegal_drugs/tobacco/e-cigarettes/vaping/alcohol',
          '2-SH': 'spam_or_harmful_content',
          '2-TR': 'terrorism',
          '2-DI': 'debated_sensitive_social_issue'}

# creates a dict {category: index}
LABEL_IDX = {label: idx for label, idx in zip(LABELS, range(len(LABELS)))}

HEADER = ['input_id', 'video_id', 'video_description', 'video_url'] + list(LABELS.values())


def save_annotations_csv(args, input_ids, classes, name):
    ''' Dump annotations to a csv file '''
    
    if args.save_annotations:
        path = "{}/{}/{}_{}_{}.csv".format(args.out_path, name, args.app_name, 
                                            args.group, name)

        with open(path, 'w', encoding='UTF8', newline='') as f:

            writer = csv.writer(f)
            writer.writerow(HEADER)

            # dump annotations for every input
            for input_id in input_ids:
                row = _from_input_to_output(input_id, input_ids[input_id], classes[input_id])
                writer.writerow(row)

        logging.info("Annotation saved to {}".format(path))


def _from_input_to_output(input_id, input, classes):
    ''' Make one output row from one input and its annotations'''
    
    meta = [input_id, input['id'], input['description'], input['url']]
    labels = _from_classes_to_labels(classes)
    # return meta + labels


def _from_classes_to_labels(classes):
    ''' Transforms a dictionary of {positive annotation: class} to a list of labels to save in output file'''
    
    labels = ['n/a'] * len(LABELS) # not in the experiment or not annotated

    if classes is not None:
        for annotation_, class_ in classes.items():
            if class_ == '_LP_':
                labels[LABEL_IDX[annotation_]] = '1' # positive
            elif class_ == '_LS_':
                labels[LABEL_IDX[annotation_]] = '0' # safe (not-category or none-of-the-above)
            elif class_ == '_LN_':
                labels[LABEL_IDX[annotation_]] = '-1' # negative but not safe

    return labels

