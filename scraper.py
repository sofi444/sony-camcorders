import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd


urls = [
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdrcx-series/hdr-cx210e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx220e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx240e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx250e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx330e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx350ve/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx505ve/specifications"
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

async def fetch(session, url):
    """
    Asynchronously fetch the page content from the given URL.
    """
    async with session.get(url, headers=HEADERS) as response:
        return await response.text()


def parse_specs(html, model_name):
    """
    Parse the specifications from the HTML.
    """
    soup = BeautifulSoup(html, 'html.parser')
    specs = {'Model': model_name}

    sections = soup.find_all('li', class_='spec-section')
    for section in sections:
        category = section.find('h2', class_='spec-section-label').get_text(strip=True)
        
        # Add category if it doesn't already exist
        if category not in specs:
            specs[category] = {}

        # Find all features within the category
        items = section.find_all('li', class_='spec-section-item')
        for item in items:
            # Feature name
            key = item.find('h3', class_='spec-section-item-header').get_text(strip=True)
            # Feature value
            value = item.find('p', class_='spec-section-item-body').get_text(strip=True)
            specs[category][key] = value
    
    return specs


async def main():
    """
    Scrape and compile data
    """
    # Start session
    async with aiohttp.ClientSession() as session:
        # Define tasks (fetch page content)
        tasks = [fetch(session, url) for url in urls]
        # Run tasks concurrently
        html_pages = await asyncio.gather(*tasks)
        
        # Extract model names from URLs and parse specs
        camcorder_specs = []
        for html, url in zip(html_pages, urls):
            model_name = url.split('/')[-2].replace('-', ' ').upper()
            specs = parse_specs(html, model_name)
            camcorder_specs.append(specs)
        
        # Flatten nested dictionaries and convert to DataFrame
        df = pd.json_normalize(camcorder_specs, sep='_')
        df.set_index('Model', inplace=True)
        
        print(df)
        df.to_csv('sony_camcorder_comparison.csv')


if __name__ == "__main__":
    asyncio.run(main())