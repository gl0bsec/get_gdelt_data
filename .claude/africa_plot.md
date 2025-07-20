# Africa Plot Generator

Generate an accurate scatterplot of Russian involvement events in African countries using GDELT data.

## What this prompt does:

1. **Loads verified African FIPS codes** from local dictionary files in report_generation/
2. **Filters Russia data** for events occurring in African countries 
3. **Creates scatterplot** showing:
   - X-axis: Average Goldstein Score (conflict/cooperation scale)
   - Y-axis: Average Tone (sentiment scale)
   - Point size: Number of events per country
   - Labels for top 15 countries by event count
4. **Outputs**:
   - `russia_africa_events_corrected_scatterplot.pdf` - The visualization
   - `russia_africa_plot_data_corrected.csv` - Aggregated data used for plotting
   - `russia_africa_events_corrected.csv` - Complete filtered dataset

## Prerequisites:
- All required files are in `report_generation/` directory:
  - `FIPS.country.txt`, `CAMEO.country.txt` (local dictionaries)
  - `country_code_utils.py` (utilities module)
  - `generate_FINAL_africa_plot.py` (main script)

## Usage:

Run the command: `/africa_plot`

This will execute the script from the report_generation directory which:
- Uses 53 verified African FIPS codes from local dictionaries
- Filters for Russian involvement events (Actor1 OR Actor2 = RUS)
- Applies proper event counting methodology
- Generates accurate visualization with country name labels

## Expected Results:

The plot will show Russian involvement events across African countries with:
- **Top countries**: South Africa, Nigeria, Egypt, Burkina Faso, Mali
- **Total events**: ~3,215 events across ~48 countries
- **Nigeria ranking**: #2 with ~391 events
- **Accurate geographic filtering**: Only genuine African countries included

## Files Generated:

- **Primary Output**: `russia_africa_events_corrected_scatterplot.pdf`
- **Data Tables**: `russia_africa_plot_data_corrected.csv`, `russia_africa_events_corrected.csv`

Execute the following Python script to generate the plot:

```python
import os
os.chdir('report_generation')
exec(open('generate_FINAL_africa_plot.py').read())
```