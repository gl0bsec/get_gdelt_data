"""Export GDELT DataFrames to geographic formats."""

import pandas as pd
from xml.sax.saxutils import escape


def to_kml(df, output_file, min_goldstein=None, max_goldstein=None,
           exclude_no_coords=True):
    """Write GDELT events to a KML file.

    Parameters
    ----------
    df : pandas.DataFrame
        GDELT events with at least ``ActionGeo_Lat`` and ``ActionGeo_Long``.
    output_file : str
        Destination ``.kml`` path.
    min_goldstein : float, optional
        Minimum Goldstein score (inclusive).
    max_goldstein : float, optional
        Maximum Goldstein score (inclusive).
    exclude_no_coords : bool
        Drop rows missing lat/lon (default ``True``).

    Returns
    -------
    pandas.DataFrame
        The (possibly filtered) DataFrame that was written.
    """
    df = df.copy()

    if min_goldstein is not None:
        df = df[df["GoldsteinScale"] >= min_goldstein]
    if max_goldstein is not None:
        df = df[df["GoldsteinScale"] <= max_goldstein]
    if exclude_no_coords:
        df = df.dropna(subset=["ActionGeo_Lat", "ActionGeo_Long"])

    if df.empty:
        print("No events match the specified criteria.")
        return df

    # Format dates
    if "DATEADDED" in df.columns:
        df["FormattedDate"] = pd.to_datetime(
            df["DATEADDED"].astype(str), format="%Y%m%d%H%M%S", errors="coerce"
        )
        df["FormattedDate"] = df["FormattedDate"].fillna(
            pd.to_datetime(df["DATEADDED"].astype(str), format="%Y%m%d", errors="coerce")
        )
        df["FormattedDate"] = df["FormattedDate"].dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        df["FormattedDate"] = "N/A"

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(_KML_HEADER)

        for _, row in df.iterrows():
            lat = row["ActionGeo_Lat"]
            lon = row["ActionGeo_Long"]
            goldstein = row.get("GoldsteinScale", "N/A")
            event_desc = str(row.get("EventDescription", "N/A"))
            location = str(row.get("ActionGeo_FullName", "Unknown Location"))
            source_url = str(row.get("SOURCEURL", "N/A"))
            date_added = str(row.get("FormattedDate", "N/A"))
            month_year = str(row.get("MonthYear", "N/A"))

            if goldstein != "N/A":
                style = "negative" if goldstein < 0 else ("positive" if goldstein > 0 else "neutral")
            else:
                style = "neutral"

            name = f"{event_desc[:50]}..." if len(event_desc) > 50 else event_desc

            f.write(f"""
    <Placemark>
      <name>{escape(name)}</name>
      <description><![CDATA[
<b>Event:</b> {escape(event_desc)}<br/>
<b>Location:</b> {escape(location)}<br/>
<b>Date Added:</b> {escape(date_added)}<br/>
<b>Month/Year:</b> {escape(str(month_year))}<br/>
<b>Goldstein Score:</b> {goldstein}<br/>
<b>Source:</b> <a href="{escape(source_url)}" target="_blank">View Article</a>
]]></description>
      <styleUrl>#{style}</styleUrl>
      <Point>
        <coordinates>{lon},{lat},0</coordinates>
      </Point>
    </Placemark>""")

        f.write(_KML_FOOTER)

    print(f"KML written: {len(df):,} placemarks -> {output_file}")
    return df


# ---------------------------------------------------------------------------
# KML boilerplate
# ---------------------------------------------------------------------------

_KML_HEADER = """\
<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>GDELT Events</name>
    <description>GDELT events visualization</description>

    <Style id="negative">
      <IconStyle>
        <color>ff0000ff</color>
        <scale>0.8</scale>
        <Icon>
          <href>http://maps.google.com/mapfiles/kml/shapes/exclamation.png</href>
        </Icon>
      </IconStyle>
    </Style>

    <Style id="positive">
      <IconStyle>
        <color>ff00ff00</color>
        <scale>0.8</scale>
        <Icon>
          <href>http://maps.google.com/mapfiles/kml/shapes/star.png</href>
        </Icon>
      </IconStyle>
    </Style>

    <Style id="neutral">
      <IconStyle>
        <color>ff00ffff</color>
        <scale>0.6</scale>
        <Icon>
          <href>http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png</href>
        </Icon>
      </IconStyle>
    </Style>
"""

_KML_FOOTER = """
  </Document>
</kml>
"""
