import os
import aiohttp
from aiohttp import ClientSession
import asyncio
import pandas as pd
import argparse

limit = 50
parser = argparse.ArgumentParser()

parser.add_argument(
    "--pages",
    type=int
)

args = parser.parse_args()

no_of_pages = args.pages

async def fetch(session, url, headers):
    async with session.get(url, headers=headers) as response:
        return await response.text()

async def bound_fetch(sem, session, url, headers):
    async with sem:
        return await fetch(session, url, headers)

async def main():
    tasks = []

    sem = asyncio.Semaphore(limit)
    async with aiohttp.ClientSession() as session:
        for page in range(1, no_of_pages+1):
            district = 'KCSW'
            url  = f"http://na.hkea.com.hk/web/blessingrealty/deal?p_p_id=DealTransaction_WAR_MyAgent_INSTANCE_TvR5&p_p_lifecycle=1&p_p_state=normal&p_p_mode=view&p_p_col_id=column-1&p_p_col_pos=1&p_p_col_count=2&_DealTransaction_WAR_MyAgent_INSTANCE_TvR5_page.pageNumber={page}&_DealTransaction_WAR_MyAgent_INSTANCE_TvR5_struts.portlet.action=%2Fdealtransaction%2Fview%2Findex&_DealTransaction_WAR_MyAgent_INSTANCE_TvR5_struts.portlet.mode=view&condition.district={district}"
            headers = {
                "Cookie":"GUEST_LANGUAGE_ID=en_US; COOKIE_SUPPORT=true;  JSESSIONID=6C8FF904B5FB0B6EBE0F9FB62FFBF55F"
            }
            task = asyncio.ensure_future(bound_fetch(sem, session, url, headers))
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks)
        return responses

def get_data_table(html):
    dfs = pd.read_html(html)
    df = dfs[0]
    return df

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    responses = loop.run_until_complete(main())

    df_all = pd.DataFrame()
    for response in responses:
        df = get_data_table(response)
        df_all = df_all.append(df, ignore_index=True)
    
    df_all.to_csv("inputs/KCSW.csv", index=False)
