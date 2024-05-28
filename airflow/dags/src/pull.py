import os
import requests
import base64
import logging
import datetime
import time
from src import MinioConnection

logger = logging.getLogger(__name__)

debug_read = True

def get_oauth_token():
    """Returns ooauth access token, which is generated with secret and key.
    
    Secret and key are defined in docker secret and are stored at API_SECRET_PATH.
    """
    # Get docker secret with API access information.
    API_SECRET_PATH = os.environ.get('API_SECRET_FILE')
    credentials = {}
    try:
        with open(API_SECRET_PATH) as file:
            exec(file.read(), credentials)
    except Exception as e:
        logger.error(f'An error during reading the secret occured: {e}')
        
    # Encode credentials
    credentials = f"{credentials['API_KEY']}:{credentials['API_SECRET']}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    # API endpoint for token request
    token_url = "https://api.idealista.com/oauth/token"

    # Headers for the token request
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }

    # Parameters for the token request
    params = {
        "grant_type": "client_credentials",
        "scope": "read"
    }

    # Make the token request
    response = requests.post(token_url, headers=headers, data=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Extract the access token from the response
        api_response = response.json()
        access_token = api_response.get("access_token")
        logger.info("Token acquired successfully.")
        return access_token
    else:
        logger.error("Failed to obtain access token. Status code:", response.status_code)
        logger.error("Response content:", response.text)
        return None
    
    
def pull_and_save_from_api():
    """Define timedelta from last pull. Based on settings and delta pull information from API.
    
    After the pull, information is stored in minio bucket.
    """
    
    # Get access token.
    access_token = get_oauth_token()
    
    # Define the headers for the property search request
    property_search_headers = {
        'Authorization': f'Bearer {access_token}'
    }
    
    # Get minio client for storing and reading data.
    minio = MinioConnection()
    
    time_from_last_pull_param = minio.get_timedelta_from_last_pull()
    
    # No pull needed
    if (time_from_last_pull_param == 0):
        return
    
    if time_from_last_pull_param is not None:
        property_search_params = {
            'country': 'es',
            'locale': 'en',
            'operation': 'rent',
            'propertyType': 'homes',
            'maxItems': 50,
            'order': 'publicationDate',
            'sort': 'desc',
            'center': "38.371622,-0.424756",
            'distance': 4500,
            'maxPrice': 1200,
            'minPrice': 500,
            'sinceDate': time_from_last_pull_param
        }
        if time_from_last_pull_param == 'AllTime':
            del property_search_params['sinceDate']
            
        # Pull data here 
        current_page = 1
        while True:
            property_search_params['numPage'] = current_page
            
            # Make the property search request using POST method
            property_search_response = requests.post('https://api.idealista.com/3.5/es/search', 
                                                    params=property_search_params, 
                                                    headers=property_search_headers)
            
            # Checking the response
            if property_search_response.status_code == 200:
                logger.info("Data pulled successfully.")
                data = property_search_response.json()
                
                # Save file as backup (in future to minIO)
                filename = 'response' \
                    + str(datetime.datetime.now().date()).replace('-','_') \
                    + '_p_' \
                    + str(current_page) \
                    + '.json'
                
                minio.write_object(minio.PULLDATA_BUCKET_NAME, filename, data)
                
                # Since idealista API allows one request per second
                time.sleep(2)
                
                if data['actualPage'] == data['totalPages']:
                    break
                else:
                    current_page += 1
            else:
                logger.error("Failed to retrieve data:", property_search_response.status_code, property_search_response.text)
                
    # Update last pull date
    minio.update_last_pull()
        
    
            