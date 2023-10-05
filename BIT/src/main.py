from leads_processor.processor import *
import json


class ETLProcess():
    def run(self, source, destination):
        # Extract
        session = LeadsProcessor()
        open_session = session.create_snowpark_session()
        company_leads_df = open_session.table(source)

        # Transform
        removed_duplicates_df = session.remove_duplicates(company_leads_df, 'ID')
        update_columns_df = session.update_columns(removed_duplicates_df)
        requested_cancel_df = session.requested_cancel(removed_duplicates_df)
        sold_leads_df = session.sold_lead(removed_duplicates_df)
        events_in_rows_df = session.union_data(update_columns_df, requested_cancel_df, sold_leads_df)

        # Add GUID
        @udtf(output_schema=StructType([StructField("id", StringType())]))
        class guid_udtf:
            def process(self) -> Iterable[Tuple[str]]:
                yield (str(uuid.uuid4()),)

        lead_events_with_UUID = events_in_rows_df.with_column("ID", guid_udtf())

        # Load
        session.write_to_table(lead_events_with_UUID, destination)
        session.create_snowpark_session().close()


if __name__ == "__main__":
    try:
        with open('config.json', 'r') as json_file:
            config = json.load(json_file)
        etl_process = ETLProcess()
        etl_process.run(source=config['table_name']['company_leads'], destination=config['table_name']['lead_events'])
    except Exception as e:
        print("Error:", str(e))
