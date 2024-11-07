import aiohttp
import asyncio
import json
from bs4 import BeautifulSoup
import pandas as pd
from process_specs import process_all_specs
from tqdm.asyncio import tqdm_asyncio
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load schema
SPECS_SCHEMA = "specs_schema.json"
try:
    with open(SPECS_SCHEMA, 'r') as f:
        SPECS_SCHEMA = json.load(f)
except Exception as e:
    logging.error(f"Error loading schema: {e}")
    logging.error(f"Schema must exist at {SPECS_SCHEMA}")
    exit(1)

urls = [
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdrcx-series/hdr-cx210e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx220e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx240e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx250e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdrcx-series/hdr-cx305e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx330e/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx350ve/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx405/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-cx-series/hdr-cx505ve/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-pj-series/hdr-pj580ve/specifications",
    "https://www.sony.co.uk/electronics/support/memory-camcorders-hdr-pj-series/hdr-pj810e/specifications"
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

async def fetch(session, url):
    """
    Asynchronously fetch the page content from the given URL.
    """
    try:
        async with session.get(url, headers=HEADERS) as response:
            if response.status != 200:
                logging.error(f"Failed to fetch {url}: Status {response.status}")
                return None
            return await response.text()
    except Exception as e:
        logging.error(f"Error fetching {url}: {str(e)}")
        return None


def parse_specs(html, model_name):
    """
    Parse the specifications from the HTML.
    """
    if not html:
        logging.warning(f"No HTML content for {model_name}")
        return None
    
    soup = BeautifulSoup(html, 'html.parser')
    specs = {'Model': model_name}

    sections = soup.find_all('li', class_='spec-section')
    if not sections:
        logging.warning(f"No sections found for {model_name}")
        return None
    
    for section in sections:
        category = section.find('h2', class_='spec-section-label').get_text(strip=True)
        specs[category] = {}

        # Find all features within the category
        items = section.find_all('li', class_='spec-section-item')
        if not items:
            logging.warning(f"No items found for {category} in {model_name}")
            continue
        
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
        html_pages = await tqdm_asyncio.gather(*tasks, desc="Fetching pages")
        
        # Parse specs
        logging.info(f"Parsing specs")
        camcorder_specs = []
        for html, url in zip(html_pages, urls):
            model_name = url.split('/')[-2].replace('-', ' ').upper()
            specs = parse_specs(html, model_name)
            if specs:
                camcorder_specs.append(specs)
            else:
                logging.warning(f"Skipping {model_name}: No specs found, parsing error")

        # Process all specs in batches
        logging.info(f"Processing specs")
        normalized_specs = await process_all_specs(camcorder_specs, SPECS_SCHEMA, batch_size=4)

        # Ensure all models are in the normalized specs
        for raw_specs, norm_specs in zip(camcorder_specs, normalized_specs):
            norm_specs['Model'] = raw_specs['Model']

        logging.debug(f"Normalized specs: {normalized_specs}")
        logging.debug(f"Raw specs: {camcorder_specs}")
        
        try:
            logging.info(f"Flattening normalized specs")
            df_normalized = pd.json_normalize(normalized_specs, sep='_')
            
            # Handle 'Model' column separately
            model_column = df_normalized['Model']
            df_normalized = df_normalized.drop('Model', axis=1)
            
            # Create multi-level columns
            columns = df_normalized.columns
            categories = [col.split('_')[0] for col in columns]
            features = [col.split('_')[1] for col in columns]
            
            # Create MultiIndex for columns
            df_normalized.columns = pd.MultiIndex.from_tuples(
                list(zip(categories, features)),
                names=['Category', 'Feature']
            )

            # Add Model back as index
            df_normalized.index = model_column
            
            # Prepare descriptions
            descriptions = ['Model']
            for feature in features:
                for category, features_dict in SPECS_SCHEMA.items():
                    if feature in features_dict:
                        description = features_dict[feature]
                        descriptions.append(f"\"{description}\"")
            
            # Save with a format that preserves the multi-column headers
            with open('normalized_specs.csv', 'w', newline='', encoding='utf-8') as f:
                # First write the category row
                unique_categories = []
                category_spans = []
                
                for cat in categories:
                    if cat not in unique_categories:
                        count = categories.count(cat)
                        unique_categories.append(cat)
                        category_spans.append(count)
                
                # Write the category row with proper spans
                f.write('Model')  # First column for index
                for cat, span in zip(unique_categories, category_spans):
                    f.write(',' + cat + ',' * (span - 1))
                f.write('\n')
                
                # Write the feature names
                f.write('Model,' + ','.join(features) + '\n')
                
                # Write the descriptions
                f.write(','.join(descriptions) + '\n')
                
                # Write the data
                df_normalized.to_csv(f, header=False)
            
            logging.info(f"Processed {len(normalized_specs)} models! Saved to CSV.")

            # Also save the raw specs as CSV
            df_raw = pd.json_normalize(camcorder_specs, sep='_')
            df_raw.set_index('Model', inplace=True)
            df_raw.to_csv('raw_specs.csv')

        except Exception as e:
            logging.error(f"Error creating DataFrames / saving to CSV: {e}")

if __name__ == "__main__":
    TEST = True
    if TEST:
        urls = urls[:1]
    
    asyncio.run(main())
