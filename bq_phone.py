import asyncio
from proxycurl.asyncio import Proxycurl, do_bulk

from google.cloud import bigquery

proxycurl = Proxycurl()
client = bigquery.Client()

def fetch_rows_from_bq():
    query = """
    SELECT linkedin_profile_url
    FROM `your_project.your_dataset.your_table`
    LIMIT 50
    """
    query_job = client.query(query)
    return [row["linkedin_profile_url"] for row in query_job]

def insert_results_to_bq(results):
    table_id = "your_project.your_dataset.your_table"
    errors = client.insert_rows_json(table_id, results)
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")

async def process_batch():
    while True:
        rows = fetch_rows_from_bq()
        if not rows:
            break

        bulk_linkedin_person_data = [(proxycurl.linkedin.person.get, {'linkedin_profile_url': row}) for row in rows]
        results = await do_bulk(bulk_linkedin_person_data)

        insert_results_to_bq(results)
        print('Batch processed and inserted to BigQuery')

async def main():
    await process_batch()

if __name__ == "__main__":
    asyncio.run(main())