from collections import namedtuple

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
}

CATEGORY_IDX = {category: idx for category, idx in zip(CATEGORIES.values(), range(len(CATEGORIES)))}

TAXONOMY = {'categories': {
                # ADULT
                'adult': {
                    'positive': [
                        '2-AD-explicit-sexual-dancemoves',
                        '2-AD-fetishes-kinks-or-bdsm',
                        '2-AD-nudity-or-partial-nudity',
                        '2-AD-sex-toys-or-aids',
                        '2-AD-sexual-activity-or-acts',
                        '2-AD-sexual-health-sex-ed', 
                        '2-AD-sexual-innuendo-or-ref',
                        '2-AD-sexual-wearing-of-clothes',
                        '2-AD-sexuality-or-sex-relations',
                        '2-AD-explicit-sexualized-dancing', # pilot
                        '2-AD-sextoys-sexaids', # pilot
                        '2-AD-sexacts', # pilot
                        '2-AD-sexual-health-sex-edu', # pilot
                        '2-AD-sexual-innuendo-reference', # pilot
                        '2-AD-sexualized-clothing', # pilot
                        '2-AD-sex-sexualrelationships', # pilot
                        '2-AD-nudity-partial-nudity', # pilot
                        '2-AD-fetish-kinks-bdsm', # pilot
                    ],
                    'safe': [
                        '2-not-adult'
                    ],
                    'aggr_positive': 'adult',
                    'aggr_safe': 'not-adult'
                },
                # CRIME
                'crime': {
                    'positive': [
                        '2-OC-violent-harm-self-others',
                        '2-OC-animal-abuse-or-cruelty',
                        '2-OC-sexual-assault',
                        '2-OC-bullying-or-harassment',
                        '2-OC-trafficking-slavery-rights',
                        '2-OC-white-collar-crimes',
                        '2-OC-damage-violation-property',
                        '2-OC-victim-support-or-recovery'
                    ],
                    'safe': [
                        '2-OC-not-crime-and-harmful-acts'
                    ],
                    'aggr_positive': 'crime',
                    'aggr_safe': 'not-crime'
                },
                # HATE SPEECH
                'hate': {
                    'positive': [
                        '2-HB-age',
                        '2-HB-class',
                        '2-HB-gender-or-sexual-orient',
                        '2-HB-physical-mental-ability',
                        '2-HB-race-or-ethnicity',
                        '2-HB-religion'
                    ],
                    'safe': [
                        '2-not-hate'
                    ],
                    'aggr_positive': 'hate',
                    'aggr_safe': 'not-hate'
                },
                # DRUGS
                'drugs': {
                    'positive': [
                        '2-AD-addiction-effects-recovery',
                        '2-AD-alcohol-consume-recognize',
                        '2-AD-drug-paraphernalia-or-acc',
                        '2-AD-drug-usage-or-pretend',
                        '2-AD-smoke-alcohol-drugs-songs',
                        '2-AD-smoke-or-vape-accessories',
                        '2-AD-smoke-or-vape-or-pretend',
                        '2-ID-smoking-drug-alc-songlyrics', # pilot
                        '2-ID-alcohol-consume-recognize', # pilot
                        '2-ID-drug-usage', # pilot
                        '2-ID-drug-paraphernalia-acc', # pilot
                        '2-ID-accessory-smoke-vape', # pilot
                        '2-ID-smoke-vape', # pilot
                        '2-ID-addiction-effects-recovery'# pilot

                    ],
                    'safe': [
                        '2-not-drugs-tobacco-vape-alcohol'
                    ],
                    'aggr_positive': 'drugs',
                    'aggr_safe': 'not-drugs'
                },
                # OBSCENITY & PROFANITY
                'obscenity': {
                    'positive': [
                        '2-OC-gross-disgust-repulsive',
                        '2-OC-vulgar-or-crass',
                        '2-OC-offensive-language',
                        '2-OC-offensive-song-lyrics',
                        '2-OC-rude-or-offensive-gestures',
                        '2-OP-gross-disgust-repulse', # pilot
                        '2-OP-mild-severe-offensive-lang', # pilot
                        '2-OP-mild-severe-offensive-song', # pilot
                        '2-OP-rude-offensive-gesture', # pilot
                        '2-OP-vulgar-crass' # pilot
                    ],
                    'safe': [
                        '2-OC-not-obscenity-and-profanity'
                    ],
                    'aggr_positive': 'obscenity',
                    'aggr_safe': 'not-obscenity'
                }
            },
            'content': [
                '1-CT-depict-describe-narrate',
                '1-CT-educate-inform-raiseaware',
                '1-CT-illustrate-animate-cartoon',
                '1-CT-joke-or-sarcasm',
                '1-CT-opinion-response-reaction',
                '1-CT-promo-perpetuation-of-hate',
                '1-CT-promo-perp-adult-drugs',
                '1-CT-promo-perp-obscene-crime',    
                '1-CT-unsure',
                '1-CT-promo-perpuate-AD-OP-ID' # pilot
            ]
}

SPECIAL_USE_LABELS = {'1-CT-nottarget-or-english': 'not_targetted_language_or_english',
                      '1-CT-dontunderstand-english': 'dont_understand_english',
                      '1-video-unavailable': 'video_unvailable'}

GT_SAFE_LABEL = 'safe'

GROUPS = {'Hate_Speech': ['hate'],
          'Group_1': ['adult', 'drugs'],
          'Group_2': ['crime', 'obscenity'],
          }

# For pilots
GROUPS.update({cat: [cat] for cat in CATEGORIES.keys()})          

def get_taxonomy_object(group_name):
    CATEGORY_OBJECT = namedtuple('CATEGORY_OBJECT', ['name', 'positive', 'safe', 'aggr_positive', 'aggr_safe'])
    TAXONOMY_OBJECT = namedtuple('TAXONOMY_OBJECT', ['categories', 'category_idx', 'content'])

    category_objects = []
    for category in GROUPS[group_name]:
        name = CATEGORIES[category]
        category_objects.append(CATEGORY_OBJECT(name,
                                                TAXONOMY['categories'][category]['positive'],
                                                TAXONOMY['categories'][category]['safe'],
                                                TAXONOMY['categories'][category]['aggr_positive'],
                                                TAXONOMY['categories'][category]['aggr_safe']))
    taxonomy_object = TAXONOMY_OBJECT(category_objects, CATEGORY_IDX, TAXONOMY['content'])

    return taxonomy_object