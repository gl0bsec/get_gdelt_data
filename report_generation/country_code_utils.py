"""
Country Code Utilities

Functions to load and work with FIPS and CAMEO country code dictionaries
from local files downloaded from GDELT.
"""

import os

def load_fips_dictionary(file_path='FIPS.country.txt'):
    """
    Load FIPS country code dictionary from local file.
    
    Args:
        file_path (str): Path to the FIPS country code file
        
    Returns:
        dict: Dictionary mapping FIPS codes to country names
    """
    fips_dict = {}
    
    if not os.path.exists(file_path):
        print(f"Error: FIPS file not found at {file_path}")
        return fips_dict
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and '\t' in line:
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        code = parts[0].strip()
                        name = parts[1].strip()
                        fips_dict[code] = name
        
        print(f"Loaded {len(fips_dict)} FIPS country codes from {file_path}")
        return fips_dict
    
    except Exception as e:
        print(f"Error loading FIPS dictionary: {e}")
        return {}

def load_cameo_dictionary(file_path='CAMEO.country.txt'):
    """
    Load CAMEO country code dictionary from local file.
    
    Args:
        file_path (str): Path to the CAMEO country code file
        
    Returns:
        dict: Dictionary mapping CAMEO codes to country names
    """
    cameo_dict = {}
    
    if not os.path.exists(file_path):
        print(f"Error: CAMEO file not found at {file_path}")
        return cameo_dict
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Skip header line
            next(f)
            for line in f:
                line = line.strip()
                if line and '\t' in line:
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        code = parts[0].strip()
                        name = parts[1].strip()
                        cameo_dict[code] = name
        
        print(f"Loaded {len(cameo_dict)} CAMEO country codes from {file_path}")
        return cameo_dict
    
    except Exception as e:
        print(f"Error loading CAMEO dictionary: {e}")
        return {}

def identify_african_countries(fips_dict):
    """
    Identify African countries from FIPS dictionary based on country names.
    
    Args:
        fips_dict (dict): FIPS country code dictionary
        
    Returns:
        list: List of FIPS codes for African countries
    """
    # Keywords that indicate African countries
    african_keywords = [
        'algeria', 'angola', 'benin', 'botswana', 'burkina', 'burundi', 'cameroon',
        'cape verde', 'central african', 'chad', 'comoros', 'congo', 'djibouti',
        'egypt', 'equatorial guinea', 'eritrea', 'ethiopia', 'gabon', 'gambia',
        'ghana', 'guinea', 'ivory coast', 'kenya', 'lesotho', 'liberia', 'libya',
        'madagascar', 'malawi', 'mali', 'mauritania', 'mauritius', 'morocco',
        'mozambique', 'namibia', 'niger', 'nigeria', 'rwanda', 'sao tome',
        'senegal', 'seychelles', 'sierra leone', 'somalia', 'south africa',
        'sudan', 'swaziland', 'tanzania', 'togo', 'tunisia', 'uganda', 'zambia',
        'zimbabwe'
    ]
    
    african_codes = []
    
    for code, name in fips_dict.items():
        name_lower = name.lower()
        # Check if any African keyword appears in the country name
        if any(keyword in name_lower for keyword in african_keywords):
            african_codes.append(code)
    
    return sorted(african_codes)

def verify_country_codes(codes_list, fips_dict, description="codes"):
    """
    Verify a list of country codes against FIPS dictionary.
    
    Args:
        codes_list (list): List of country codes to verify
        fips_dict (dict): FIPS country code dictionary
        description (str): Description of the code list for output
        
    Returns:
        tuple: (valid_codes, invalid_codes, non_african_codes)
    """
    valid_codes = []
    invalid_codes = []
    non_african_codes = []
    
    # Non-African indicators
    non_african_indicators = [
        'china', 'united states', 'russia', 'japan', 'south korea', 'singapore',
        'chile', 'switzerland', 'germany', 'france', 'united kingdom', 'italy',
        'spain', 'portugal', 'netherlands', 'belgium', 'austria', 'sweden',
        'norway', 'denmark', 'finland', 'poland', 'czech', 'slovakia',
        'hungary', 'romania', 'bulgaria', 'croatia', 'slovenia', 'estonia',
        'latvia', 'lithuania', 'ukraine', 'belarus', 'moldova', 'armenia',
        'azerbaijan', 'georgia', 'kazakhstan', 'uzbekistan', 'turkmenistan',
        'kyrgyzstan', 'tajikistan', 'afghanistan', 'pakistan', 'india',
        'bangladesh', 'myanmar', 'thailand', 'vietnam', 'cambodia', 'laos',
        'malaysia', 'indonesia', 'philippines', 'brunei', 'mongolia',
        'australia', 'new zealand', 'papua new guinea', 'fiji', 'samoa',
        'tonga', 'vanuatu', 'solomon islands', 'canada', 'mexico', 'guatemala',
        'belize', 'honduras', 'el salvador', 'nicaragua', 'costa rica',
        'panama', 'cuba', 'jamaica', 'haiti', 'dominican republic', 'bahamas',
        'barbados', 'trinidad', 'grenada', 'antigua', 'dominica', 'saint lucia',
        'saint vincent', 'saint kitts', 'brazil', 'argentina', 'colombia',
        'venezuela', 'guyana', 'suriname', 'ecuador', 'peru', 'bolivia',
        'paraguay', 'uruguay', 'iran', 'iraq', 'turkey', 'syria', 'lebanon',
        'israel', 'jordan', 'saudi arabia', 'yemen', 'oman', 'united arab',
        'qatar', 'bahrain', 'kuwait', 'cyprus', 'greece', 'albania', 'serbia',
        'bosnia', 'montenegro', 'macedonia', 'kosovo', 'liechtenstein', 'monaco',
        'san marino', 'andorra', 'malta', 'iceland', 'ireland', 'guam',
        'puerto rico', 'virgin islands', 'american samoa', 'marshall islands',
        'palau', 'micronesia', 'nauru', 'kiribati', 'tuvalu'
    ]
    
    for code in codes_list:
        if code in fips_dict:
            name = fips_dict[code].lower()
            if any(indicator in name for indicator in non_african_indicators):
                non_african_codes.append((code, fips_dict[code]))
            else:
                valid_codes.append(code)
        else:
            invalid_codes.append(code)
    
    return valid_codes, invalid_codes, non_african_codes

if __name__ == "__main__":
    # Test the functions
    print("=== Testing Country Code Utilities ===")
    
    # Load dictionaries
    fips_dict = load_fips_dictionary()
    cameo_dict = load_cameo_dictionary()
    
    # Identify African countries
    african_codes = identify_african_countries(fips_dict)
    print(f"\\nIdentified {len(african_codes)} African countries:")
    for code in african_codes:
        print(f"  {code}: {fips_dict[code]}")
    
    # Test current list from script
    current_list = [
        'AG', 'AO', 'BC', 'BN', 'BY', 'CD', 'CF', 'CG', 'CM', 'CN', 'CT', 'CV', 'DJ', 'EG', 
        'EK', 'ER', 'ET', 'GA', 'GB', 'GH', 'GV', 'KE', 'LI', 'LT', 'LY', 'MA', 'MI', 'ML', 
        'MO', 'MP', 'MR', 'MZ', 'NG', 'NI', 'OD', 'PU', 'RW', 'SE', 'SF', 'SG', 'SL', 'SO', 
        'SU', 'TO', 'TP', 'TS', 'TZ', 'UG', 'UV', 'WA', 'WZ', 'ZA', 'ZI'
    ]
    
    print(f"\\n=== Verifying Current Script List ===")
    valid, invalid, non_african = verify_country_codes(current_list, fips_dict, "current script list")
    
    print(f"Valid African codes: {len(valid)}")
    print(f"Invalid codes: {len(invalid)}")
    if invalid:
        print(f"  Invalid: {invalid}")
    
    print(f"Non-African codes: {len(non_african)}")
    if non_african:
        print("  Non-African countries found:")
        for code, name in non_african:
            print(f"    {code}: {name}")