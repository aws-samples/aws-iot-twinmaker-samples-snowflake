# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2021
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from . import udqw_constants, get_value

class UDQParamsValidator:

    def validate(self, params):
        # validate required fields
        UDQParamsValidator.validate_required_field(params, udqw_constants.START_TIME)
        UDQParamsValidator.validate_required_field(params, udqw_constants.END_TIME)
        UDQParamsValidator.validate_required_field(params, udqw_constants.WORKSPACE_ID)
        UDQParamsValidator.validate_required_field(params, udqw_constants.SELECTED_PROPERTIES)

        # entityId and componentName must show hand in hand
        entity_id = get_value(params, udqw_constants.ENTITY_ID)
        component_name = get_value(params, udqw_constants.COMPONENT_NAME)
        if (entity_id and not component_name) or (not entity_id and component_name):
            raise Exception("EntityId and componentName must show up together")

        # validate the selected properties
        selected_properties = get_value(params, udqw_constants.SELECTED_PROPERTIES)
        if (len(selected_properties) != 1) or (selected_properties[0] != udqw_constants.FILTER_ALARM_PROPERTY_NAME):
            raise Exception('Unexpected selectedProperties[{}]'.format(selected_properties))

    @staticmethod
    def validate_required_field(dict, key):
        if key not in dict:
            raise Exception("Required key[{}] is missing".format(key))