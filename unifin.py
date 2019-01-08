
"""API FRM-unifin, local access only"""

import hug


@hug.local()
def FRM_unifin(name: hug.types.text, age: hug.types.number, hug_timer=3):
    """Says happy birthday to a user"""
    return {'message': 'Happy {0} Birthday {1}!'.format(age, name),
            'took': float(hug_timer)}