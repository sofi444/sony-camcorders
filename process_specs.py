import asyncio
import json
import logging
import os
import time

import aiohttp
from dotenv import load_dotenv
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL")


SYS_PROMPT = f"""You are an expert in camcorders and data normalization, tasked with transforming camcorder specifications into a standardized format for seamless comparison across different camcorder models.

OBJECTIVE:
Using the provided raw specifications, extract and normalize data into a predefined schema with categories and features, focusing on accuracy and consistency. The goal is to ensure that each feature is captured uniformly across all models, enabling direct comparison. For example, information about the aperture of the lens will be present in all model specifications, but it may appear under different categories and/or feature names. Your task is to identify and extract the feature value from the raw specs and place it under the correct category and feature name in the schema.

RULES:
1. Extract ONLY information available in the raw specifications. Do not infer or generate missing values.
2. For any missing feature, use an empty string ("").
3. Standardize formatting and terminology:
   - Remove brand names unless explicitly part of the specification (e.g., "ZEISS®" for Lens Type).
   - Use semicolons to separate multiple values (e.g., "50i; 25p"). Ensure all available options for a feature are listed.
   - Avoid unnecessary characters (e.g., remove parentheses unless essential for context).

SPECIFIC GUIDELINES:
- Express aperture as "f/" followed by the value (e.g., "f/1.8").
- Use mm for sensor sizes
- Capture focal lengths in 35mm equivalent
- Frame rate options: List all supported frame rates (e.g., "24p; 30p; 50i; 60p")
- Physical: Convert all measurements to mm and grams. Provide the weight information including battery. If only the weight of the camera body is provided, indicate it by adding "(body only)" to the weight value.
- Other features: List only confirmed and notable features, that actually give a real advantage to the user (e.g., integrated projector etc.)

OUTPUT REQUIREMENTS:
1. Generate a JSON object that matches the schema structure exactly.
2. Ensure all values are strings and adhere to consistent formatting.
3. Include each category and feature from the schema, even if empty.
4. Do not include feature descriptions in your output; only provide extracted values.

"""

PROMPT = f"""
INPUT SCHEMA:
{{schema}}

RAW SPECIFICATIONS:
{{raw_specs}}

Please provide the normalized specifications in JSON format:"""


async def process_specs_batch(specs_batch, schema):
    """
    Process a batch of raw specs and generate JSON with normalized features.
    """
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for raw_specs in specs_batch:
            
            formatted_prompt = PROMPT.format(
                raw_specs=json.dumps(raw_specs, indent=4), 
                schema=json.dumps(schema, indent=4)
            )

            data = {
                "model": OPENAI_MODEL,
                "messages": [
                    {"role": "system", "content": SYS_PROMPT},
                    {"role": "user", "content": formatted_prompt}
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.1
            }
            
            tasks.append(session.post("https://api.openai.com/v1/chat/completions", headers=headers, json=data))
        
        responses = await tqdm_asyncio.gather(*tasks, desc="Processing batch")
        normalized_specs = []
        for response in responses:
            response_data = await response.json()
            try:
                normalized_specs.append(
                    json.loads(response_data['choices'][0]['message']['content'])
                )
            except Exception as e:
                print(f"Error processing response: {response_data}")
                raise e
        
        return normalized_specs

async def process_all_specs(specs_list, schema, batch_size=16):
    """
    Process all specs in batches.
    """
    results = []
    for i in tqdm(range(0, len(specs_list), batch_size), desc="Processing batches"):
        batch = specs_list[i:i + batch_size]
        batch_results = await process_specs_batch(batch, schema)
        results.extend(batch_results)
        logging.info(f"Sleeping for 5 seconds to avoid rate limit...")
        await asyncio.sleep(5)
    return results
