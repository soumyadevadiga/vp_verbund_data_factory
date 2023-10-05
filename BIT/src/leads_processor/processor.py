from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.dataframe import *
from snowflake.snowpark.functions import udtf
from snowflake.snowpark.types import StructType, StructField, StringType
import json
import uuid


def load_connection_param():
    """
    Reads the connection parameters from connection.json and makes it accessible as global variables
    :return: Dictionary whose value can accessed with key
    """
    try:
        with open('connection.json', 'r') as json_file:
            conn = json.load(json_file)
        return conn
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print("Error with connection file:", str(e))


def load_config_values():
    try:
        with open('config.json', 'r') as json_file:
            config = json.load(json_file)
        return config
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print("Error with config file:", str(e))


class LeadsProcessor:
    """Represents the processes needed for the Leads Lifecycle

    It has the methods to connect to Snowflake database through a snowpark session.
    It has the methods to transform the Leads data whose life cycle is described in multiple
    columns to different rows for data for individual life cycle event of Leads
    """

    def __init__(self):
        # self.session = self.create_snowpark_session()
        self.config = load_config_values()


    def create_snowpark_session(self):
        try:
            conn = load_connection_param()
            connection_params = {
                "account": conn['account'],
                "user": conn['user'],
                "password": conn['password'],
                "role": conn['role'],
                "warehouse": conn['warehouse'],
                "database": conn['database'],
                "schema": conn['schema']
            }
            session = Session.builder.configs(connection_params).create()
            return session
        except Exception as e:
            print("Error :", str(e))

    def remove_duplicates(self, dataframe, column_name):
        """
        Removes duplicates by checking duplicates in the specified column
        :param dataframe: source data from table
        :param column_name: column on which the duplicates are checked
        :return: data without duplicates
        """
        try:
            return dataframe.dropDuplicates(str(column_name))
        except Exception as e:
            print("Error in removing duplicates :", str(e))

    def update_columns(self, dataframe):
        """
        Updates the new columns for the existing rows in the source dataframe
        with event conditional values

        :param dataframe : Source data cleansed with removal of duplicates
        :return dataframe : Updated columns
        """
        try:
            updated_df = dataframe.rename(col(self.config['column_name']['id']), self.config['column_value']['id_new_value']) \
                .select(when(col(self.config['column_name']['state']) == 0, lit(self.config['column_value']['event_type_state_0'])).
                        when(col(self.config['column_name']['state']) == 1, lit(self.config['column_value']['event_type_state_1'])).
                        when(col(self.config['column_name']['state']) == 2, lit(self.config['column_value']['event_type_state_2'])).
                        when(col(self.config['column_name']['state']) == 3, lit(self.config['column_value']['event_type_state_3'])).
                        as_(self.config['column_name']['event_type']),
                        when(col(self.config['column_name']['state']) == 0, col(self.config['column_name']['sold_emp'])).
                        when(col(self.config['column_name']['state']) == 1, lit(self.config['column_value']['event_employee_state_1_or_3'])).
                        when(col(self.config['column_name']['state']) == 2, col(self.config['column_name']['cancelled_emp'])).
                        when(col(self.config['column_name']['state']) == 3, lit(self.config['column_value']['event_employee_state_1_or_3']))
                        .as_(self.config['column_name']['event_emp']),
                        when(col(self.config['column_name']['state']) == 0, col(self.config['column_name']['created_dt'])).
                        when(col(self.config['column_name']['state']) == 1, col(self.config['column_name']['cancel_request_dt'])).
                        when(col(self.config['column_name']['state']) == 2, col(self.config['column_name']['cancelled_dt'])).
                        when(col(self.config['column_name']['state']) == 3, col(self.config['column_name']['cancel_reject_dt']))
                        .as_(self.config['column_name']['event_dt']), col(self.config['column_value']['id_new_value']), col(self.config['column_name']['updated_dt']))
            return updated_df
        except Exception as e:
            print("Error in updating columns for existing rows:", str(e))

    def requested_cancel(self, dataframe):
        """
        Creates the set of rows conditionally for the lead requested cancellation event

        :param dataframe : Source data cleansed with removal of duplicates
        :return dataframe : Intermediate data with rows for lead requested for cancellation event info
        """
        try:
            requested_cancel_df = dataframe \
                .filter(col(self.config['column_name']['state']) > 1) \
                .rename(self.config['column_name']['id'], self.config['column_value']['id_new_value']) \
                .with_columns([self.config['column_name']['event_type'], self.config['column_name']['event_emp'], self.config['column_name']['event_dt']],
                              [lit(self.config['column_value']['event_type_state_1']), lit(self.config['column_value']['event_employee_state_1_or_3']), col(self.config['column_name']['updated_dt'])]) \
                .select(self.config['column_name']['event_type'], self.config['column_name']['event_emp'], self.config['column_name']['event_dt'], self.config['column_value']['id_new_value'], self.config['column_name']['updated_dt'])
            return requested_cancel_df
        except Exception as e:
            print("Error in creating rows for lead requested cancellation event: " , str(e))

    def sold_lead(self, dataframe):
        """
        Create the set of rows conditionally for the lead sold event
        :param dataframe: Source data cleansed with removal of duplicates
        :return: Intermediate data with rows for Lead sold event event
        """
        try:
            sold_lead_df = dataframe \
                .filter(col(self.config['column_name']['state']) > 0) \
                .rename(self.config['column_name']['id'], self.config['column_value']['id_new_value']) \
                .with_columns([self.config['column_name']['event_type'], self.config['column_name']['event_emp'], self.config['column_name']['event_dt']],
                              [lit(self.config['column_value']['event_type_state_0']), col(self.config['column_name']['sold_emp']), col(self.config['column_name']['created_dt'])]) \
                .select(self.config['column_name']['event_type'], self.config['column_name']['event_emp'], self.config['column_name']['event_dt'], self.config['column_value']['id_new_value'], self.config['column_name']['updated_dt'])
            return sold_lead_df
        except Exception as e:
            print("Error in creating rows for Lead Sold event: ", str(e))

    def union_data(self, df1, df2, df3):
        """

        :param df1: Existing Source Data with updated column values
        :param df2: Data for Lead Requested Cancellation Event
        :param df3: Data for Lead Sold event
        :return: Data with individual rows for all 4 stages of Lead lifecycle
        """
        try:
            return df1.union(df2).union(df3)
        except Exception as e:
            print("Error while combining the dataframes:", str(e))


    def write_to_table(self, dataframe, table_name):
        """
        Uploads the final processed data to snowflake table
        :param dataframe: Data with processed rows
        :param table_name: Destination table in snowflake
        :return:
        """
        try:
            return dataframe.write.mode(self.config['options']['write_method']).save_as_table(table_name)
        except Exception as e:
            print("Error while loading the output dataframe to destination table: ", str(e))
