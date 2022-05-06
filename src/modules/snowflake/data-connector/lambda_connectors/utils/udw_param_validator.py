# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2021
# SPDX-License-Identifier: Apache-2.0

from . import udqw_constants, get_value

class UDWParamsValidator:

    def validate(self, params):
        # validate required fields
        UDWParamsValidator.validate_required_field(params, udqw_constants.PROPERTIES)
        UDWParamsValidator.validate_required_field(params, udqw_constants.ENTRIES)

        properties = get_value(params, udqw_constants.PROPERTIES)
        entries = get_value(params, udqw_constants.ENTRIES)

        for entry in entries:
            UDWParamsValidator.validate_required_field(entry, udqw_constants.ENTITY_PROPERTY_REFERENCE)
            entity_property_reference = get_value(entry, udqw_constants.ENTITY_PROPERTY_REFERENCE)

            UDWParamsValidator.validate_required_field(entity_property_reference, udqw_constants.ENTITY_ID)
            UDWParamsValidator.validate_required_field(entity_property_reference, udqw_constants.PROPERTY_NAME)

            entity_id = get_value(entity_property_reference, udqw_constants.ENTITY_ID)
            property_name = get_value(entity_property_reference, udqw_constants.PROPERTY_NAME)

            UDWParamsValidator.validate_property_configuration(entity_id, property_name, properties)

    @staticmethod
    def validate_property_configuration(entity_id, property_name, properties):
        UDWParamsValidator.validate_required_field(properties, entity_id)
        entity_property = get_value(properties, entity_id)
        UDWParamsValidator.validate_required_field(entity_property, property_name)

    @staticmethod
    def validate_required_field(dict, key):
        if key not in dict:
            raise Exception("Required key ['{}'] is missing".format(key))
