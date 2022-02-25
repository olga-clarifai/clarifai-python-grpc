import json
import matplotlib.pyplot as plt

ENG_path = ''
DE_path = ''
FR_path = ''

CATEGORIES = {'adult': 'adult_&_explicit_sexual_content',
              'arms': 'arms_&_ammunition',
              'crime': 'crime',
              'death': 'death,_injury_or_military_conflict',
              'hate': 'hate_speech',
              'drugs': 'illegal_drugs/tobacco/e-cigarettes/vaping/alcohol',
              'obscenity': 'obscenity_&_profanity',
              'piracy': 'online_piracy',        
              'social': 'debated_sensitive_social_issue',      
              'spam': 'spam_or_harmful_content',
              'terrorism': 'terrorism',
              'other': 'safe'
}

CATEGORY_ABBR = {value: key for key, value in CATEGORIES.items()}


def extract_gt_labels(path):

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
    ax1.set_xticklabels([])
    ax1.set_xticks([])
    ax1.set_xlim([0, max(ENG_labels.values()) + 20])
    ax1.title.set_text('ENG')
    for i, v in enumerate(ENG_labels.values()):
        ax1.text(v + 3, i - 0.1, str(v))

    ax2.barh(list(DE_labels.keys()), list(DE_labels.values())) 
    # ax2.set_yticklabels([])
    # ax2.set_yticks([])
    ax2.set_xticklabels([])
    ax2.set_xticks([])
    ax2.set_xlim([0, max(DE_labels.values()) + 40])
    ax2.title.set_text('DE')
    for i, v in enumerate(DE_labels.values()):
        ax2.text(v + 6, i - 0.1, str(v))

    ax3.barh(list(FR_labels.keys()), list(FR_labels.values())) 
    # ax3.set_yticklabels([])
    # ax3.set_yticks([])
    ax3.set_xticklabels([])
    ax3.set_xticks([])
    ax3.set_xlim([0, max(FR_labels.values()) + 10])
    ax3.title.set_text('FR')
    for i, v in enumerate(FR_labels.values()):
        ax3.text(v + 1, i - 0.1, str(v))

    plt.subplots_adjust(wspace = 6)
    fig.tight_layout()
    plt.show()  


# ----------- MAIN ---------- #
ENG_labels = extract_gt_labels(ENG_path)
DE_labels = extract_gt_labels(DE_path)
FR_labels = extract_gt_labels(FR_path)

plot_distribution(ENG_labels, DE_labels, FR_labels)
