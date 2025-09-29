#!/usr/bin/env python3
"""
Test Legacy Polygon.io Financials Endpoint
"""

import asyncio
import json
import os
import aiohttp
from dotenv import load_dotenv

load_dotenv('/workspaces/data-collection-service/.env')

async def test_legacy_financials():
    api_key = os.getenv('POLYGON_API_KEY')
    ticker = 'AAPL'

    # Try the legacy endpoint that might still work with Starter plan
    url = f"https://api.polygon.io/v2/reference/financials/{ticker}"
    params = {
        'apikey': api_key,
        'limit': 4,
        'type': 'Y'
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            print(f"Status: {response.status}")
            text = await response.text()
            print(f"Response: {text}")

            if response.status == 200:
                data = await response.json()
                print(f"JSON Keys: {list(data.keys())}")
                if 'results' in data and data['results']:
                    print("✅ Legacy financials endpoint works!")
                    result = data['results'][0]
                    print(f"Sample financial data keys: {list(result.keys())}")
                else:
                    print("❌ No results in legacy financials")
            else:
                print("❌ Legacy financials endpoint failed")

if __name__ == "__main__":
    asyncio.run(test_legacy_financials())