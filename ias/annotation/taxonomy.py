from collections import namedtuple

CATEGORIES = {'adult': 'adult_&_explicit_sexual_content',
              'arms': 'arms_&_ammunition',
              'crime': 'crime',
              'death': 'death,_injury_or_military_conflict',
              'social': 'debated_sensitive_social_issue',
              'hate': 'hate_speech',
              'drugs': 'illegal_drugs/tobacco/e-cigarettes/vaping/alcohol',
              'obscenity': 'obscenity_&_profanity',
              'piracy': 'online_piracy',              
              'spam': 'spam_or_harmful_content',
              'terrorism': 'terrorism',
}

CATEGORY_IDX = {category: idx for category, idx in zip(CATEGORIES.values(), range(len(CATEGORIES)))}

TAXONOMY = {'categories': {
                # ADULT
                'adult': {
                    'positive': {
                        '2-AD-explicit-sexual-dancemoves',
                        '2-AD-fetishes-kinks-or-bdsm',
                        '2-AD-nudity-or-partial-nudity',
                        '2-AD-sex-toys-or-aids',
                        '2-AD-sexual-activity-or-acts',
                        '2-AD-sexual-health-sex-ed',
                        '2-AD-sexual-innuendo-or-ref',
                        '2-AD-sexual-wearing-of-clothes',
                        '2-AD-sexuality-or-sex-relations'
                    },
                    'safe': {
                        '2-not-adult'
                    },
                    'aggr_positive': 'adult',
                    'aggr_safe': 'not-adult'
                },
                # HATE SPEECH
                'hate': {
                    'positive': {
                        '2-HB-age',
                        '2-HB-class',
                        '2-HB-gender-or-sexual-orient',
                        '2-HB-physical-mental-ability',
                        '2-HB-race-or-ethnicity',
                        '2-HB-religion'
                    },
                    'safe': {
                        '2-not-hate'
                    },
                    'aggr_positive': 'hate',
                    'aggr_safe': 'not-hate'
                },
                # DRUGS
                'drugs': {
                    'positive': {
                        '2-AD-addiction-effects-recovery',
                        '2-AD-alcohol-consume-recognize',
                        '2-AD-drug-paraphernalia-or-acc',
                        '2-AD-drug-usage-or-pretend',
                        '2-AD-smoke-alcohol-drugs-songs',
                        '2-AD-smoke-or-vape-accessories',
                        '2-AD-smoke-or-vape-or-pretend'
                    },
                    'safe': {
                        '2-not-drugs-tobacco-vape-alcohol'
                    },
                    'aggr_positive': 'drugs',
                    'aggr_safe': 'not-drugs'
                }
            },
            'content': {
                '1-CT-depict-describe-narrate',
                '1-CT-educate-inform-raiseaware',
                '1-CT-illustrate-animate-cartoon',
                '1-CT-joke-or-sarcasm',
                '1-CT-opinion-response-reaction',
                '1-CT-promo-perpetuation-of-hate',
                '1-CT-promo-perp-adult-drugs',
                '1-CT-unsure'
            }
}

SPECIAL_USE_LABELS = {'1-CT-nottarget-or-english': 'not_targetted_language_or_english',
                      '1-CT-dontunderstand-english': 'dont_understand_english',
                      '1-video-unavailable': 'video_unvailable'}

GROUPS = {'Hate_Speech': ['hate'],
          'Group_1': ['adult', 'drugs'],
          'Group_2': ['crime', 'obscenity']}

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