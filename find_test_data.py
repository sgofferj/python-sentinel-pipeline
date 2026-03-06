import os
import copernicus as cop
from dotenv import load_dotenv
from shapely.wkt import loads
from datetime import datetime, timedelta

load_dotenv()

USERNAME = os.getenv("COPERNICUS_USERNAME")
PASSWORD = os.getenv("COPERNICUS_PASSWORD")
mycop = cop.connect(USERNAME, PASSWORD)

# Southern Finland box from .env
BOX_SFIN = "21.099243,59.770226,25.477295,61.543641"

def find_test_candidates():
    print("Searching for S2 L2A candidates (Jan-Feb 2026, <5% clouds)...")
    _, s2_result = mycop.productSearch(
        "Sentinel2",
        box=BOX_SFIN,
        startDate="2026-01-01T00:00:00Z",
        cloudCover=5,
        productType="L2A",
        maxRecords=50
    )
    
    # Filter for Feb 28 and earlier
    s2_features = [f for f in s2_result['features'] if f['properties']['startDate'] < "2026-03-01"]
    print(f"Found {len(s2_features)} S2 features.")

    print("\nSearching for S1 GRD IW candidates (Jan-Feb 2026)...")
    _, s1_result = mycop.productSearch(
        "Sentinel1",
        box=BOX_SFIN,
        startDate="2026-01-01T00:00:00Z",
        productType="GRD",
        sensorMode="IW",
        maxRecords=100
    )
    
    s1_features = [f for f in s1_result['features'] if f['properties']['startDate'] < "2026-03-01"]
    print(f"Found {len(s1_features)} S1 features.")

    print("\nCorrelating...")
    matches = []
    for s2 in s2_features:
        s2_time = datetime.fromisoformat(s2['properties']['startDate'].replace('Z', '+00:00'))
        s2_geom = loads(s2['properties']['footprint'])
        s2_title = s2['properties']['title']
        
        for s1 in s1_features:
            s1_time = datetime.fromisoformat(s1['properties']['startDate'].replace('Z', '+00:00'))
            s1_geom = loads(s1['properties']['footprint'])
            s1_title = s1['properties']['title']
            
            time_diff = abs(s1_time - s2_time)
            if time_diff < timedelta(hours=24):
                if s2_geom.intersects(s1_geom):
                    matches.append({
                        's2_id': s2['id'],
                        's2_title': s2_title,
                        's2_time': s2_time,
                        's2_clouds': s2['properties']['cloudCover'],
                        's1_id': s1['id'],
                        's1_title': s1_title,
                        's1_time': s1_time,
                        'time_diff': time_diff
                    })
    
    # Sort by time difference
    matches.sort(key=lambda x: x['time_diff'])
    
    for m in matches[:10]:
        print(f"MATCH (diff: {m['time_diff']}):")
        print(f"  S2: {m['s2_title']} ({m['s2_clouds']}% clouds)")
        print(f"  S1: {m['s1_title']}")
        print(f"  IDs: S2={m['s2_id']} | S1={m['s1_id']}")
        print("-" * 40)

if __name__ == "__main__":
    find_test_candidates()
