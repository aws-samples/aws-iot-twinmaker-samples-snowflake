# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2021
# SPDX-License-Identifier: Apache-2.0

from . import udqw_constants, get_value
from datetime import datetime

class UDQWParamsParser:

    def __init__(self, event):
        self.event = event

    def get_workspace_id(self):
        return get_value(self.event, udqw_constants.WORKSPACE_ID)

    def get_entity_id(self):
        return get_value(self.event, udqw_constants.ENTITY_ID)

    def get_component_name(self):
        return get_value(self.event, udqw_constants.COMPONENT_NAME)

    def get_properties(self):
        return get_value(self.event, udqw_constants.PROPERTIES)

    def get_selected_properties(self):
        return get_value(self.event, udqw_constants.SELECTED_PROPERTIES)

    def get_entries(self):
        return get_value(self.event, udqw_constants.ENTRIES)

    def get_next_token(self):
        return get_value(self.event, udqw_constants.NEXT_TOKEN)

    def get_max_results(self):
        return get_value(self.event, udqw_constants.MAX_RESULTS)

    def get_alarm_filter_status(self):
        filters = get_value(self.event, udqw_constants.FILTERS)
        if not filters or len(filters) != 1:
            return None

        filter = filters[0]
        if (filter[udqw_constants.FILTER_OPERATOR] == '=') and (filter[udqw_constants.FILTER_PROPERTY_NAME] == udqw_constants.FILTER_ALARM_PROPERTY_NAME):
            return filter[udqw_constants.FILTER_VALUE][udqw_constants.FILTER_STRING_VALUE]

        return None

    def get_order_by(self):
        order_by_time = get_value(self.event, udqw_constants.ORDER_BY_TIME)
        if not order_by_time:
            return udqw_constants.ORDER_BY_ASC

        if order_by_time == udqw_constants.ORDER_BY_ASCENDING:
            order_by = udqw_constants.ORDER_BY_ASC
        elif order_by_time == udqw_constants.ORDER_BY_DESCENDING:
            order_by = udqw_constants.ORDER_BY_DESC
        else:
            order_by = udqw_constants.ORDER_BY_ASC
        return order_by

    def get_start_time(self):
        return get_value(self.event, udqw_constants.START_TIME)

    def get_end_time(self):
        return get_value(self.event, udqw_constants.END_TIME)

    def get_alarm_id(self):
        component_properties = self.get_properties()
        if not component_properties:
            return None

        alarm_id = None
        if udqw_constants.ALARM_KEY in component_properties:
            alarm_id = component_properties[udqw_constants.ALARM_KEY][udqw_constants.FILTER_VALUE][udqw_constants.FILTER_STRING_VALUE]

        return alarm_id

    def get_table_name(self):
        table_name = None
        component_properties = self.get_properties()
        if not component_properties:
            return None

        if udqw_constants.TABLE_NAME in component_properties:
            table_name = component_properties[udqw_constants.TABLE_NAME][udqw_constants.FILTER_VALUE][udqw_constants.FILTER_STRING_VALUE]

        return table_name

    def get_timeseries_table_name(self):
        component_properties = self.get_properties()
        if udqw_constants.TIMESERIES_TABLE_NAME in component_properties:
            return component_properties[udqw_constants.TIMESERIES_TABLE_NAME][udqw_constants.PROPERTY_VALUE][udqw_constants.PROPERTY_STRING_VALUE]
        else:
            return None

    def get_attribute_property_table_name(self):
        component_properties = self.get_properties()
        if udqw_constants.ATTRIBUTE_PROPERTY_TABLE_NAME in component_properties:
            return component_properties[udqw_constants.ATTRIBUTE_PROPERTY_TABLE_NAME][udqw_constants.PROPERTY_VALUE][udqw_constants.PROPERTY_STRING_VALUE]
        else:
            return None

    def get_element_id(self):
        component_properties = self.get_properties()
        if udqw_constants.ELEMENT_ID in component_properties:
            return component_properties[udqw_constants.ELEMENT_ID][udqw_constants.PROPERTY_VALUE][udqw_constants.PROPERTY_STRING_VALUE]
        else:
            return None
