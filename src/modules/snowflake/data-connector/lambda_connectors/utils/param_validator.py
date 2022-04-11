from datetime import datetime
from . import udqw_constants, get_value


# TODO make the validator generic


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

        # validate startDateTime and endDateTime
        start_date_time = get_value(params, udqw_constants.START_TIME)
        UDQParamsValidator.validate_timestamp(start_date_time)
        end_date_time = get_value(params, udqw_constants.END_TIME)
        UDQParamsValidator.validate_timestamp(end_date_time)

    @staticmethod
    def validate_required_field(dict, key):
        if key not in dict:
            raise Exception("Required key[{}] is missing".format(key))

    @staticmethod
    def validate_timestamp(date_string):
        try:
            datetime.fromisoformat(date_string)
        except:
            raise Exception("{} is not a valid ISO timestamp string".format(date_string))
