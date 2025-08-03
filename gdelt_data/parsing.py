
import re
import time
from typing import Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

## Normal URLs version 
def extract_url_metadata(url: str, timeout: int = 10) -> Dict[str, Optional[str]]:
    """
    Extract metadata from a webpage URL including title, description, and other relevant information.
    """
    metadata = {
        'url': url,
        'title': None,
        'description': None,
        'keywords': None,
        'author': None,
        'site_name': None,
        'image': None,
        'favicon': None,
        'canonical_url': None,
        'language': None,
        'content_type': None,
        'status_code': None,
        'error': None
    }

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        metadata['status_code'] = response.status_code
        metadata['content_type'] = response.headers.get('content-type', '')

        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title
        title_tag = soup.find('title')
        if title_tag:
            metadata['title'] = title_tag.get_text().strip()

        # Extract meta tags
        meta_tags = soup.find_all('meta')

        for tag in meta_tags:
            if tag.get('name', '').lower() == 'description':
                metadata['description'] = tag.get('content', '').strip()
            elif tag.get('name', '').lower() == 'keywords':
                metadata['keywords'] = tag.get('content', '').strip()
            elif tag.get('name', '').lower() == 'author':
                metadata['author'] = tag.get('content', '').strip()
            elif tag.get('name', '').lower() == 'language':
                metadata['language'] = tag.get('content', '').strip()
            elif tag.get('property', '').lower() == 'og:title' and not metadata['title']:
                metadata['title'] = tag.get('content', '').strip()
            elif tag.get('property', '').lower() == 'og:description' and not metadata['description']:
                metadata['description'] = tag.get('content', '').strip()
            elif tag.get('property', '').lower() == 'og:site_name':
                metadata['site_name'] = tag.get('content', '').strip()
            elif tag.get('property', '').lower() == 'og:image':
                metadata['image'] = tag.get('content', '').strip()
            elif tag.get('name', '').lower() == 'twitter:title' and not metadata['title']:
                metadata['title'] = tag.get('content', '').strip()
            elif tag.get('name', '').lower() == 'twitter:description' and not metadata['description']:
                metadata['description'] = tag.get('content', '').strip()
            elif tag.get('name', '').lower() == 'twitter:image' and not metadata['image']:
                metadata['image'] = tag.get('content', '').strip()

        # Extract canonical URL
        canonical_link = soup.find('link', {'rel': 'canonical'})
        if canonical_link:
            metadata['canonical_url'] = canonical_link.get('href', '').strip()

        # Extract favicon
        favicon_link = soup.find('link', {'rel': 'icon'}) or soup.find('link', {'rel': 'shortcut icon'})
        if favicon_link:
            favicon_href = favicon_link.get('href', '')
            if favicon_href:
                metadata['favicon'] = urljoin(url, favicon_href)

        # Extract language from html tag if not found in meta
        if not metadata['language']:
            html_tag = soup.find('html')
            if html_tag:
                metadata['language'] = html_tag.get('lang', '').strip()

        # Convert relative URLs to absolute
        if metadata['image'] and not metadata['image'].startswith(('http://', 'https://')):
            metadata['image'] = urljoin(url, metadata['image'])

        if metadata['canonical_url'] and not metadata['canonical_url'].startswith(('http://', 'https://')):
            metadata['canonical_url'] = urljoin(url, metadata['canonical_url'])

    except requests.exceptions.RequestException as e:
        metadata['error'] = f"Request error: {str(e)}"
    except Exception as e:
        metadata['error'] = f"Parsing error: {str(e)}"

    # Clean up empty values
    metadata = {k: v if v else None for k, v in metadata.items()}

    return metadata

## TODO Add URL content scraper 


## Gdelt verison 
def get_source_urls_with_metadata(df, actor1_code=None, actor2_code=None, geo_code=None,
                                 match_type='any', limit=None, show_events=True,
                                 extract_metadata=False, max_workers=5, delay=1.0,
                                 timeout=10, dataF=False):
    """
    Get source URLs with optional metadata extraction for events matching country code criteria.

    Parameters:
    - df: GDELT DataFrame
    - actor1_code: Country code for Actor1 (None = any country)
    - actor2_code: Country code for Actor2 (None = any country)
    - geo_code: Country code for event location (None = any location)
    - match_type: 'any' (OR) or 'all' (AND) for multiple conditions
    - limit: Maximum number of rows/URLs to return (None = all)
    - show_events: Whether to include event descriptions (True/False)
    - extract_metadata: Whether to extract webpage metadata (True/False)
    - max_workers: Number of concurrent threads for metadata extraction
    - delay: Delay between requests in seconds (to be respectful)
    - timeout: Request timeout in seconds
    - dataF: Return a DataFrame with full event details and metadata

    Returns:
    - DataFrame if dataF=True (includes metadata columns if extract_metadata=True)
    - List of tuples with metadata if extract_metadata=True
    - List of tuples (url, event_descriptions) if show_events=True
    - List of URLs if show_events=False
    """

    # Build conditions
    conditions = []

    if actor1_code:
        conditions.append(df['Actor1CountryCode'] == actor1_code)
    if actor2_code:
        conditions.append(df['Actor2CountryCode'] == actor2_code)
    if geo_code:
        conditions.append(df['ActionGeo_CountryCode'] == geo_code)

    # Apply conditions or use entire dataset if no filters specified
    if not conditions:
        print("No filters specified. Processing entire dataset...")
        filtered_df = df.copy()
    else:
        # Apply conditions based on match_type
        if match_type == 'any':
            mask = conditions[0]
            for condition in conditions[1:]:
                mask = mask | condition
        else:  # 'all'
            mask = conditions[0]
            for condition in conditions[1:]:
                mask = mask & condition

        # Filter dataframe
        filtered_df = df[mask].copy()

    # Check if we have any data to process
    if len(filtered_df) == 0:
        print("No events found matching the specified criteria.")
        if dataF:
            return pd.DataFrame()
        return []

    if dataF:
        # Aggregate GDELT variables by URL
        print("Aggregating GDELT events by URL...")

        # Convert SQLDATE to datetime first for aggregation
        filtered_df['SQLDATE_dt'] = pd.to_datetime(filtered_df['SQLDATE'], format='%Y%m%d')

        # Define aggregation functions for each column
        agg_dict = {
            'GoldsteinScale': ['mean', 'min', 'max', 'std', 'count'],  # Multiple stats for sentiment
            'Actor1Name': lambda x: ' | '.join(x.dropna().unique()[:5]),  # Top 5 unique actors
            'Actor2Name': lambda x: ' | '.join(x.dropna().unique()[:5]),  # Top 5 unique actors
            'Actor1CountryCode': lambda x: ' | '.join(x.dropna().unique()),  # All unique countries
            'Actor2CountryCode': lambda x: ' | '.join(x.dropna().unique()),  # All unique countries
            'ActionGeo_CountryCode': lambda x: ' | '.join(x.dropna().unique()),  # All unique locations
            'SQLDATE_dt': ['min', 'max'],  # Date range
            'EventDescription': lambda x: list(x.dropna().unique()) if 'EventDescription' in filtered_df.columns else []
        }

        # Perform aggregation
        result_df = filtered_df.groupby('SOURCEURL').agg(agg_dict).reset_index()

        # Flatten column names
        result_df.columns = ['_'.join(col).strip('_') if col[1] else col[0]
                           for col in result_df.columns.values]

        # Rename columns for clarity
        column_mapping = {
            'GoldsteinScale_mean': 'avg_goldstein_score',
            'GoldsteinScale_min': 'min_goldstein_score',
            'GoldsteinScale_max': 'max_goldstein_score',
            'GoldsteinScale_std': 'goldstein_score_std',
            'GoldsteinScale_count': 'event_count',
            'Actor1Name_<lambda>': 'actor1_names',
            'Actor2Name_<lambda>': 'actor2_names',
            'Actor1CountryCode_<lambda>': 'actor1_countries',
            'Actor2CountryCode_<lambda>': 'actor2_countries',
            'ActionGeo_CountryCode_<lambda>': 'event_locations',
            'SQLDATE_dt_min': 'first_event_date',
            'SQLDATE_dt_max': 'last_event_date',
            'EventDescription_<lambda>': 'event_descriptions'
        }

        # Apply column mapping
        for old_col, new_col in column_mapping.items():
            if old_col in result_df.columns:
                result_df = result_df.rename(columns={old_col: new_col})

        # Convert dates to string format
        if 'first_event_date' in result_df.columns:
            result_df['first_event_date'] = result_df['first_event_date'].dt.strftime('%Y-%m-%d')
        if 'last_event_date' in result_df.columns:
            result_df['last_event_date'] = result_df['last_event_date'].dt.strftime('%Y-%m-%d')

        # Add derived metrics
        if 'first_event_date' in result_df.columns and 'last_event_date' in result_df.columns:
            result_df['date_span_days'] = (
                pd.to_datetime(result_df['last_event_date']) -
                pd.to_datetime(result_df['first_event_date'])
            ).dt.days

        # Fill NaN standard deviations with 0 (single event cases)
        if 'goldstein_score_std' in result_df.columns:
            result_df['goldstein_score_std'] = result_df['goldstein_score_std'].fillna(0)

        # Sort by event count (most events first)
        if 'event_count' in result_df.columns:
            result_df = result_df.sort_values('event_count', ascending=False)

        # Apply limit if specified
        if limit:
            result_df = result_df.head(limit)

        # Extract metadata if requested
        if extract_metadata:
            print(f"Extracting metadata for {len(result_df)} URLs...")
            metadata_results = []

            def extract_with_delay(url):
                time.sleep(delay)
                return extract_url_metadata(url, timeout)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_url = {executor.submit(extract_with_delay, url): url
                               for url in result_df['SOURCEURL'].unique()}

                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        metadata = future.result()
                        metadata_results.append(metadata)
                    except Exception as exc:
                        print(f'URL {url} generated an exception: {exc}')
                        metadata_results.append({'url': url, 'error': str(exc)})

            # Convert metadata to DataFrame and merge
            metadata_df = pd.DataFrame(metadata_results)
            result_df = result_df.merge(metadata_df, left_on='SOURCEURL', right_on='url', how='left')

            # Drop duplicate url column
            if 'url' in result_df.columns:
                result_df = result_df.drop('url', axis=1)

        print(f"Found {len(result_df)} unique URLs from {len(filtered_df)} events")
        return result_df

    elif show_events:
        # Group by URL and get unique event descriptions and first date
        url_events = filtered_df.groupby('SOURCEURL').agg({
            'EventDescription': lambda x: list(x.dropna().unique()) if 'EventDescription' in filtered_df.columns else [],
            'SQLDATE': 'first'
        }).reset_index()

        # Convert SQLDATE to cosmograph-friendly format (YYYY-MM-DD)
        url_events['SQLDATE'] = pd.to_datetime(url_events['SQLDATE'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        # Add event count for sorting
        url_events['event_count'] = url_events['EventDescription'].apply(len)
        url_events = url_events.sort_values('event_count', ascending=False)

        # Apply limit if specified
        if limit:
            url_events = url_events.head(limit)

        print(f"Found {len(filtered_df)} events with {len(url_events)} unique URLs")

        if extract_metadata:
            print(f"Extracting metadata for {len(url_events)} URLs...")

            def extract_metadata_with_context(row):
                time.sleep(delay)
                url, events, date = row['SOURCEURL'], row['EventDescription'], row['SQLDATE']
                metadata = extract_url_metadata(url, timeout)
                return (url, events, date, metadata)

            results_with_metadata = []

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(extract_metadata_with_context, row)
                          for _, row in url_events.iterrows()]

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results_with_metadata.append(result)
                    except Exception as exc:
                        print(f'Metadata extraction failed: {exc}')

            return results_with_metadata
        else:
            # Return list of tuples (url, events, date)
            return list(zip(url_events['SOURCEURL'], url_events['EventDescription'], url_events['SQLDATE']))

    else:
        # Original behavior - just URLs
        urls = filtered_df['SOURCEURL'].dropna().unique()

        if limit:
            urls = urls[:limit]

        print(f"Found {len(filtered_df)} events with {len(urls)} unique URLs")

        if extract_metadata:
            print(f"Extracting metadata for {len(urls)} URLs...")

            def extract_with_delay(url):
                time.sleep(delay)
                return (url, extract_url_metadata(url, timeout))

            results_with_metadata = []

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(extract_with_delay, url) for url in urls]

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results_with_metadata.append(result)
                    except Exception as exc:
                        print(f'Metadata extraction failed: {exc}')

            return results_with_metadata
        else:
            return urls.tolist()

# Additional utility function for analyzing metadata
def analyze_source_metadata(df_with_metadata):
    """
    Analyze the extracted metadata to provide insights
    """
    if 'site_name' not in df_with_metadata.columns:
        print("No metadata found in DataFrame")
        return

    print("=== Metadata Analysis ===")

    # Most common news sources
    print("\nTop news sources:")
    site_counts = df_with_metadata['site_name'].value_counts().head(10)
    for site, count in site_counts.items():
        if site:
            print(f"  {site}: {count} articles")

    # Language distribution
    if 'language' in df_with_metadata.columns:
        print("\nLanguage distribution:")
        lang_counts = df_with_metadata['language'].value_counts().head(5)
        for lang, count in lang_counts.items():
            if lang:
                print(f"  {lang}: {count} articles")

    # Success rate
    total_urls = len(df_with_metadata)
    successful = len(df_with_metadata[df_with_metadata['title'].notna()])
    print(f"\nMetadata extraction success rate: {successful}/{total_urls} ({successful/total_urls*100:.1f}%)")


def combine_multiple_columns(df, column_names, new_col_name='combined_text',
                           include_labels=True, separator='\n\n'):
    """
    Combine multiple text columns from a dataframe into a new column with formatted output.

    Parameters:
    -----------
    df : pd.DataFrame
        The input dataframe
    column_names : list
        List of column names to combine in order
    new_col_name : str
        Name for the new combined column (default: 'combined_text')
    include_labels : bool
        Whether to include column names as labels (default: True)
    separator : str
        Separator between text sections (default: '\n\n')

    Returns:
    --------
    pd.DataFrame
        The original dataframe with the new combined column added
    """

    # Create a copy to avoid modifying the original dataframe
    result_df = df.copy()

    # Validate column names
    for col_name in column_names:
        if col_name not in df.columns:
            raise ValueError(f"Column '{col_name}' not found in dataframe")

    def format_content(row):
        """Format the content for a single row"""
        parts = []

        for col_name in column_names:
            val = row[col_name]
            if pd.notna(val) and str(val).strip():
                if include_labels:
                    parts.append(f"{col_name}: {val}")
                else:
                    parts.append(str(val))

        # Join parts with separator
        return separator.join(parts) if parts else ""

    # Apply the formatting function to create the new column
    result_df[new_col_name] = df.apply(format_content, axis=1)

    return result_df

def map_event_codes(df, event_dict, verbose=True):
    """
    Maps event codes to descriptions and cleans the EventCode column.
    
    Parameters:
    df (DataFrame): DataFrame containing EventCode column
    event_dict (dict): Dictionary mapping event codes to descriptions
    verbose (bool): Whether to print diagnostic information
    
    Returns:
    DataFrame: DataFrame with cleaned EventCode and new EventDescription column
    """
    # Create a copy to avoid modifying original
    result_df = df.copy()
    
    # Clean and convert EventCode to string
    if result_df['EventCode'].dtype in ['int64', 'int32', 'float64']:
        result_df['EventCode'] = result_df['EventCode'].astype(int).astype(str)
    else:
        result_df['EventCode'] = result_df['EventCode'].astype(str).str.strip()
    
    if verbose:
        # Diagnostic information
        print(f"\nEventCode data type: {result_df['EventCode'].dtype}")
        print("Sample EventCodes from your data:")
        print(result_df['EventCode'].value_counts().head())
        
        # Check if codes exist in dictionary
        print("\nChecking if your codes exist in the dictionary:")
        unique_codes = result_df['EventCode'].unique()[:10]
        for code in unique_codes:
            if code in event_dict:
                print(f"Code '{code}': Found - {event_dict[code]}")
            else:
                print(f"Code '{code}': NOT FOUND")
    
    # Map the event descriptions
    result_df['EventDescription'] = result_df['EventCode'].map(event_dict)
    
    return result_df

def parse_cameo_codes(file_path):
    """Parse the CAMEO codes file and return a dictionary mapping codes to descriptions"""

    # Read the file
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split into lines and skip the header
    lines = content.strip().split('\n')[1:]  # Skip header line

    event_dict = {}

    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue

        # Remove leading/trailing whitespace
        line = line.strip()

        # Split by multiple spaces to separate code from description
        # Use regex to split on 2+ spaces to handle the formatting
        parts = re.split(r'\s{2,}', line, 1)

        if len(parts) == 2:
            code = parts[0].strip()
            description = parts[1].strip()
            event_dict[code] = description
        else:
            # Handle cases where there might be different spacing
            # Split on first space sequence and take first part as code
            match = re.match(r'(\S+)\s+(.+)', line)
            if match:
                code = match.group(1).strip()
                description = match.group(2).strip()
                event_dict[code] = description

    return event_dict