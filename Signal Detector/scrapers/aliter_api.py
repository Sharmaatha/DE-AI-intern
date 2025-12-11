import requests
import json
import logging
import os
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_company_data_from_api(company_url: str):
    """
    Takes a company URL and returns the JSON response from a local mock file.
    """
    print(f"Fetching external data for: {company_url}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(current_dir, "mock.json")
    
    try:
        with open(filename, 'r') as f:
            mock_response = json.load(f)
            logger.info(f"Successfully loaded mock data from {filename}")
            return mock_response
            
    except FileNotFoundError:
        logger.error(f"Error: The file '{filename}' was not found.")
        return {}
    except json.JSONDecodeError:
        logger.error(f"Error: Failed to decode JSON from '{filename}'. Check the file format.")
        return {}