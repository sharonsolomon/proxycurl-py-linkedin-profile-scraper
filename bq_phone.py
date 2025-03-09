import asyncio
from proxycurl.asyncio import Proxycurl, do_bulk

from google.cloud import bigquery

import pandas as pd
import pandas_gbq


proxycurl = Proxycurl()
client = bigquery.Client()

#Change limit to # needed

def fetch_rows_from_bq():
    query = "SELECT phone as phone " \
    "FROM `ml-data-332522.innovation_engineering.SocialRCT_wi_universe_unmatched` " \
    "WHERE phone not in " \
    "(select phone from " \
    "`ml-data-332522.innovation_engineering.SocialRCT_wi_proxycurl_phone_api`)" \
    "LIMIT 1500"

    query_job = client.query(query)
    return [row["phone"] for row in query_job]

def insert_results_to_bq(results):

    # Create an empty list to store the processed data
    data = []

    # Iterate through the results
    for result in results:
        # Extract data from the Result object
        phone = result.phone_number
        linkedin_url = result.value.get('linkedin_profile_url')
        twitter_url = result.value.get('twitter_profile_url')
        facebook_url = result.value.get('facebook_profile_url')
        error = result.error

        # Append the data to the list
        data.append({
            'phone': phone,
            'linkedin_profile_url': linkedin_url,
            'twitter_profile_url': twitter_url,
            'facebook_profile_url': facebook_url,
            'error': error
        })

    # Create the DataFrame
    df = pd.DataFrame(data, columns=['phone', 'linkedin_profile_url', 'twitter_profile_url', 'facebook_profile_url', 'error'])
# To see the output, run the code.

    table_id = "ml-data-332522.innovation_engineering.SocialRCT_wi_proxycurl_phone_api"
    df.to_gbq(table_id, if_exists='append')
    print(f"Inserted {len(df)} rows into {table_id}")


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
