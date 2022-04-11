from . import udqw_constants
import json


def get_value(dict, key):
    if key in dict:
        return dict[key]
    else:
        return None

def parse_next_token(token, selectedProperties):
    '''
    For properties that still have more data points to return, the property will be attached with its last exclusive timestamp.
    So in the current query, the result will continue from last exclusive timestamp.
    For properties that have no more data points left, they'll not appear in the query.

    Token schema:

    {
        "propertyName": <lastExclusiveTimestamp> // in ISO format
    }
    '''

    if token is None or len(token.strip()) == 0:
        return None

    propertyNextTokens = None
    try:
        propertyNextTokens = json.loads(token)
    except Exception as e:
        raise Exception('Cannot decode next token: {}'.format(token), e)

    if len(propertyNextTokens.keys()) > 0:
        for propertyName, lastExclusiveTimestamp in propertyNextTokens.items():
            if propertyName not in selectedProperties:
                raise ValueError('Next token {} doesn\'t match selected properties {}'.format(token, ', '.join(selectedProperties)))
        return propertyNextTokens
    else:
        raise ValueError('Parsed next token is empty {}'.format(str(propertyNextTokens)))