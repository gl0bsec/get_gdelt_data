import gdelt
import pandas as pd
from datetime import datetime, timedelta
import time
import gc
import os
import json
import yaml
import re

class FilterRuleParser:
    """Parse plaintext filter rules into pandas operations"""
    
    def __init__(self):
        self.operators = {
            'greater than': '>',
            'greater than or equal': '>=',
            'less than': '<',
            'less than or equal': '<=',
            'equals': '==',
            'not equals': '!=',
            'contains': 'contains',
            'not contains': 'not contains',
            'in': 'in',
            'not in': 'not in',
            'is null': 'isnull',
            'is not null': 'notnull',
            'between': 'between'
        }
    
    def parse_rule(self, rule_text):
        """Parse a single plaintext rule into a filter function"""
        rule_text = rule_text.strip().lower()
        
        # Pattern for: "column operator value"
        pattern = r'(\w+)\s+(.*?)\s+(.+?)$'
        match = re.match(pattern, rule_text)
        
        if not match:
            raise ValueError(f"Could not parse rule: {rule_text}")
        
        column, operator_text, value_text = match.groups()
        
        # Find matching operator
        operator = None
        for op_key, op_val in self.operators.items():
            if op_key in operator_text:
                operator = op_val
                break
        
        if not operator:
            raise ValueError(f"Unknown operator in rule: {rule_text}")
        
        # Parse value
        value = self._parse_value(value_text)
        
        return column.upper(), operator, value
    
    def _parse_value(self, value_text):
        """Parse value from text"""
        value_text = value_text.strip()
        
        # Check for lists
        if value_text.startswith('[') and value_text.endswith(']'):
            items = value_text[1:-1].split(',')
            return [self._parse_single_value(item.strip()) for item in items]
        
        # Check for between values
        if ' and ' in value_text:
            parts = value_text.split(' and ')
            return [self._parse_single_value(parts[0]), self._parse_single_value(parts[1])]
        
        return self._parse_single_value(value_text)
    
    def _parse_single_value(self, value):
        """Parse a single value"""
        value = value.strip().strip('"\'')
        
        # Try to convert to number
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            # Return as string
            return value

def create_filter_function(filter_rules):
    """Create a filter function from plaintext rules"""
    parser = FilterRuleParser()
    
    def filter_events(df):
        original_len = len(df)
        
        for rule_name, rule_config in filter_rules.items():
            if not rule_config.get('enabled', True):
                continue
            
            rule_text = rule_config['rule']
            description = rule_config.get('description', '')
            
            try:
                column, operator, value = parser.parse_rule(rule_text)
                
                # Apply filter based on operator
                if column in df.columns:
                    if operator == '>':
                        df = df[df[column] > value]
                    elif operator == '>=':
                        df = df[df[column] >= value]
                    elif operator == '<':
                        df = df[df[column] < value]
                    elif operator == '<=':
                        df = df[df[column] <= value]
                    elif operator == '==':
                        df = df[df[column] == value]
                    elif operator == '!=':
                        df = df[df[column] != value]
                    elif operator == 'contains':
                        df = df[df[column].str.contains(value, case=False, na=False)]
                    elif operator == 'not contains':
                        df = df[~df[column].str.contains(value, case=False, na=False)]
                    elif operator == 'in':
                        df = df[df[column].isin(value)]
                    elif operator == 'not in':
                        df = df[~df[column].isin(value)]
                    elif operator == 'isnull':
                        df = df[df[column].isnull()]
                    elif operator == 'notnull':
                        df = df[df[column].notna()]
                    elif operator == 'between':
                        df = df[(df[column] >= value[0]) & (df[column] <= value[1])]
                    
                    if description:
                        print(f"    Applied: {description}")
                else:
                    print(f"    Warning: Column {column} not found for rule: {rule_name}")
            
            except Exception as e:
                print(f"    Error applying rule {rule_name}: {e}")
        
        filtered_len = len(df)
        if original_len > 0:
            print(f"    Filtered: {original_len} → {filtered_len} events ({filtered_len/original_len*100:.1f}% kept)")
        
        return df
    
    return filter_events

# Example filter configuration
# FILTER_RULES = {
#     "high_mention_events": {
#         "rule": "NumMentions greater than or equal 5",
#         "description": "Keep only events with 5+ mentions",
#         "enabled": True
#     },
#     "has_location": {
#         "rule": "ActionGeo_Lat is not null",
#         "description": "Keep only events with geographic coordinates",
#         "enabled": True
#     },
#     "has_actors": {
#         "rule": "Actor1Name is not null",
#         "description": "Keep only events with identified actors",
#         "enabled": True
#     },
#     "goldstein_range": {
#         "rule": "GoldsteinScale between -10 and 10",
#         "description": "Keep events with moderate Goldstein scale",
#         "enabled": True
#     },
#     "specific_countries": {
#         "rule": "ActionGeo_CountryCode in [US, UK, FR, DE, CN]",
#         "description": "Keep only events in specific countries",
#         "enabled": False  # Disabled by default
#     },
#     "exclude_event_types": {
#         "rule": "EventRootCode not in [20, 21, 22, 23]",
#         "description": "Exclude certain event types",
#         "enabled": False
#     },
#     "recent_sources": {
#         "rule": "NumSources greater than 2",
#         "description": "Keep events with multiple sources",
#         "enabled": True
#     }
# }

def load_filter_rules_from_file(filepath):
    """Load filter rules from a JSON or YAML file"""
    if filepath.endswith('.json'):
        with open(filepath, 'r') as f:
            return json.load(f)
    elif filepath.endswith('.yaml') or filepath.endswith('.yml'):
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    else:
        raise ValueError("Unsupported file format. Use .json or .yaml")

def save_filter_rules_template(filepath='filter_rules_template.yaml'):
    """Save a template filter rules file"""
    template = {
        "filter_rules": {
            "high_mention_events": {
                "rule": "NumMentions greater than or equal 5",
                "description": "Keep only events with 5+ mentions",
                "enabled": True
            },
            "has_location": {
                "rule": "ActionGeo_Lat is not null",
                "description": "Keep only events with geographic coordinates",
                "enabled": True
            },
            "tone_filter": {
                "rule": "AvgTone between -15 and 15",
                "description": "Remove extreme tone outliers",
                "enabled": False
            },
            "country_filter": {
                "rule": "ActionGeo_CountryCode in [US, UK, FR]",
                "description": "Keep only specific countries",
                "enabled": False
            }
        },
        "examples": {
            "numeric_comparisons": [
                "NumMentions greater than 10",
                "GoldsteinScale less than -5",
                "NumSources greater than or equal 3"
            ],
            "text_operations": [
                "Actor1Name contains protest",
                "Actor1Name not contains military",
                "EventCode in [030, 031, 032]"
            ],
            "null_checks": [
                "ActionGeo_Lat is not null",
                "Actor2Name is null"
            ],
            "ranges": [
                "SQLDATE between 20250301 and 20250331",
                "AvgTone between -10 and 10"
            ]
        }
    }
    
    with open(filepath, 'w') as f:
        yaml.dump(template, f, default_flow_style=False, sort_keys=False)
    
    print(f"Filter rules template saved to: {filepath}")

# Modified main function
def collect_gdelt_data(
    start_date,
    end_date,
    filter_rules=None,
    filter_rules_file=None,
    columns_to_keep=None,
    output_file='gdelt_events_filtered.parquet',
    batch_size=7,
    sleep_time=0.5
):
    """
    Collect GDELT data with customizable filters
    
    Parameters:
    -----------
    start_date : datetime
        Start date for data collection
    end_date : datetime
        End date for data collection
    filter_rules : dict
        Dictionary of filter rules
    filter_rules_file : str
        Path to JSON/YAML file containing filter rules
    columns_to_keep : list
        List of columns to keep in the output
    output_file : str
        Output parquet file path
    batch_size : int
        Number of days to process before saving
    sleep_time : float
        Seconds to wait between API calls
    """
    
    # Load filter rules
    if filter_rules_file:
        filter_rules = load_filter_rules_from_file(filter_rules_file)
        if 'filter_rules' in filter_rules:
            filter_rules = filter_rules['filter_rules']
    elif filter_rules is None:
        filter_rules = FILTER_RULES
    
    # Create filter function
    filter_function = create_filter_function(filter_rules)
    
    # Default columns if not specified
    if columns_to_keep is None:
        columns_to_keep = [
            'GLOBALEVENTID', 'SQLDATE', 'Actor1Name', 'Actor2Name',
            'Actor1CountryCode', 'Actor2CountryCode', 'Actor1Type1Code',
            'EventCode', 'EventRootCode', 'EventBaseCode', 'GoldsteinScale',
            'NumMentions', 'NumSources', 'NumArticles', 'AvgTone',
            'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_CountryCode',
            'ActionGeo_ADM1Code', 'ActionGeo_FullName', 'SOURCEURL'
        ]
    
    # Initialize GDELT
    gd2 = gdelt.gdelt(version=1)
    
    # Generate list of dates
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    total_dates = len(date_range)
    
    # Initialize tracking variables
    batch_results = []
    first_batch = True
    
    print(f"Starting data collection for {total_dates} days...")
    print(f"Output file: {output_file}")
    print(f"Active filters: {sum(1 for r in filter_rules.values() if r.get('enabled', True))}")
    print("-" * 60)
    
    # Loop through each date
    for i, date in enumerate(date_range):
        date_str = date.strftime('%Y %b %d')
        
        try:
            # Query for single day
            daily_results = gd2.Search(date_str, table='events', coverage=True)
            
            if daily_results is not None and len(daily_results) > 0:
                # Keep only selected columns
                available_cols = [col for col in columns_to_keep if col in daily_results.columns]
                daily_results = daily_results[available_cols]
                
                # Apply filters
                daily_results = filter_function(daily_results)
                
                # Add to batch
                if len(daily_results) > 0:
                    batch_results.append(daily_results)
                
                print(f"[{i+1}/{total_dates}] {date_str}: {len(daily_results)} events after filtering")
            else:
                print(f"[{i+1}/{total_dates}] {date_str}: No data returned")
            
            # Process batch when it reaches size limit or at the end
            if len(batch_results) >= batch_size or (i == total_dates - 1 and batch_results):
                if batch_results:
                    print(f"\n  Processing batch of {len(batch_results)} days...")
                    
                    # Combine batch
                    batch_df = pd.concat(batch_results, ignore_index=True)
                    
                    # Optimize dtypes
                    batch_df = optimize_dtypes(batch_df)
                    
                    # Save to file
                    if first_batch:
                        batch_df.to_parquet(output_file, compression='snappy', index=False)
                        first_batch = False
                        print(f"  Created new file with {len(batch_df)} events")
                    else:
                        existing_df = pd.read_parquet(output_file)
                        combined_df = pd.concat([existing_df, batch_df], ignore_index=True)
                        combined_df.to_parquet(output_file, compression='snappy', index=False)
                        print(f"  Appended {len(batch_df)} events (total in file: {len(combined_df)})")
                        del existing_df, combined_df
                    
                    # Clear batch
                    batch_results = []
                    del batch_df
                    gc.collect()
                    
                    print("-" * 60)
            
            # Sleep to avoid overwhelming API
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"[{i+1}/{total_dates}] Error on {date_str}: {e}")
            continue
    
    # Final summary
    print("\n" + "=" * 60)
    print("DATA COLLECTION COMPLETE")
    print("=" * 60)
    
    if os.path.exists(output_file):
        final_df = pd.read_parquet(output_file)
        print(f"Total events collected: {len(final_df):,}")
        print(f"File size: {os.path.getsize(output_file) / (1024*1024):.2f} MB")
        print(f"Date range: {final_df['SQLDATE'].min()} to {final_df['SQLDATE'].max()}")

def optimize_dtypes(df):
    """Optimize DataFrame dtypes to reduce memory usage"""
    # [Previous optimize_dtypes function remains the same]
    float_cols = df.select_dtypes(include=['float64']).columns
    for col in float_cols:
        df[col] = df[col].astype('float32')
    
    int_cols = df.select_dtypes(include=['int64']).columns
    for col in int_cols:
        col_min = df[col].min()
        col_max = df[col].max()
        if col_min >= -128 and col_max <= 127:
            df[col] = df[col].astype('int8')
        elif col_min >= -32768 and col_max <= 32767:
            df[col] = df[col].astype('int16')
        elif col_min >= -2147483648 and col_max <= 2147483647:
            df[col] = df[col].astype('int32')
    
    for col in ['Actor1CountryCode', 'Actor2CountryCode', 'ActionGeo_CountryCode', 
                'EventCode', 'EventRootCode', 'EventBaseCode']:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].astype('category')
    
    return df

def interactive_filter_builder():
    """Interactive tool to build filter rules"""
    filters = {}
    
    print("=== GDELT Filter Builder ===")
    print("Build custom filters for your GDELT data collection\n")
    
    while True:
        print("\nCurrent filters:")
        if not filters:
            print("  (none)")
        else:
            for name, rule in filters.items():
                status = "ON" if rule['enabled'] else "OFF"
                print(f"  - {name}: {rule['rule']} [{status}]")
        
        print("\nOptions:")
        print("1. Add new filter")
        print("2. Toggle filter on/off")
        print("3. Remove filter")
        print("4. Save filters to file")
        print("5. Test filters on sample data")
        print("6. Done")
        
        choice = input("\nChoice (1-6): ").strip()
        
        if choice == '1':
            name = input("Filter name: ").strip()
            print("\nExamples:")
            print("  NumMentions greater than 5")
            print("  Actor1Name contains protest")
            print("  ActionGeo_CountryCode in [US, UK, FR]")
            print("  GoldsteinScale between -5 and 5")
            
            rule = input("Filter rule: ").strip()
            desc = input("Description (optional): ").strip()
            
            filters[name] = {
                'rule': rule,
                'description': desc,
                'enabled': True
            }
            
        elif choice == '2' and filters:
            name = input("Filter name to toggle: ").strip()
            if name in filters:
                filters[name]['enabled'] = not filters[name]['enabled']
                
        elif choice == '3' and filters:
            name = input("Filter name to remove: ").strip()
            if name in filters:
                del filters[name]
                
        elif choice == '4' and filters:
            filename = input("Save as (e.g., my_filters.yaml): ").strip()
            with open(filename, 'w') as f:
                yaml.dump({'filter_rules': filters}, f, default_flow_style=False)
            print(f"Saved to {filename}")
            
        elif choice == '5':
            # Test filters on sample data
            print("\nTesting filters on sample data...")
            # Add test implementation here
            
        elif choice == '6':
            break
    
    return filters

# Run the interactive builder
# filters = interactive_filter_builder()

# 1. Basic usage with default filters
collect_gdelt_data(
    start_date=datetime(2025, 3, 1),
    end_date=datetime(2025, 3, 31)
)

# 2. Custom filters for protest events
protest_filters = {
    # "protest_events": {
    #     "rule": "EventRootCode equals 14",
    #     "description": "Protest events",
    #     "enabled": True
    # },
    "high_visibility": {
        "rule": "NumMentions greater than 5",
        "description": "Highly reported events",
        "enabled": True
    }
}

collect_gdelt_data(
    start_date=datetime(2025, 3, 1),
    end_date=datetime(2025, 3, 31),
    filter_rules=protest_filters,
    output_file='gdelt_protests_march2025.parquet'
)

# # 3. Use saved filter configuration
# collect_gdelt_data(
#     start_date=datetime(2025, 3, 1),
#     end_date=datetime(2025, 6, 5),
#     filter_rules_file='gdelt_filters.yaml'
# )
