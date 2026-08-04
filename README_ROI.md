# Region of Interest (ROI) Management

The Python Sentinel Pipeline includes tools for managing Regions of Interest (ROIs). This allows for automated cropping of specific geographic areas across different satellite products (Sentinel-1, Sentinel-2, Fused), along with automated notifications and social media updates.

## ROI Manager (`roi_manager.py`)

The `roi_manager.py` script is responsible for scanning the pipeline inventory and extracting crops for ROIs defined in `roi_config.json`.

### Key Capabilities

- **Automated Cropping:** Extracts crops from source TIFFs based on WGS84 bounding boxes.
- **Smart Mosaicing:** Automatically groups source tiles by date and orbit (relative orbit + direction) to handle ROIs that span multiple tiles.
- **Single-Tile Optimization:** If a single source tile provides sufficient coverage for an ROI, the system skips mosaicing entirely to reduce compute and avoid potential band artifacts.
- **Coverage Filtering:** Only generates a crop if the available satellite data covers a minimum percentage of the ROI (defined by `bbox_match`).
- **Social Media Integration:** Automatically posts ROI updates to **Bluesky** with rich text and functional hashtags.
- **Notifications:** Sends high-quality JPEG previews to mobile devices or other services via **Apprise**.
- **Metadata Propagation:** ROI crops inherit all relevant metadata from parent products, including cloud cover, acquisition time, and orbit information.

### Source Data Requirements

**Important:** The ROI Manager does not download data itself. It crops from the existing pipeline inventory. For an ROI to be processed:
1. Its geographic area must be contained within (or overlap) the primary bounding boxes defined in your `.env` file (`S1_BOX` and/or `S2_BOX`).
2. The pipeline must have successfully downloaded and rendered the parent products for those areas.

If you define an ROI outside of your global `.env` boxes, no data will be available for cropping.

### Usage

The ROI manager is typically run as part of the main pipeline, but can be invoked manually:

```bash
# Process only new products since the last run
python roi_manager.py

# Process ALL historical products in the inventory (WARNING: Triggers notifications/posts)
python roi_manager.py --all

# Re-run a single ROI against a specific acquisition date (re-crops and re-notifies)
python roi_manager.py --roi <NAME> --date YYYY-MM-DD

# Preview what a re-run would do without cropping or sending anything
python roi_manager.py --roi <NAME> --date YYYY-MM-DD --dry-run
```

---

## ROI Configuration (`roi_config.json`)

The `roi_config.json` file defines your regions and global settings.

### Global Configuration (`config`)

| Key | Description |
| :--- | :--- |
| `roi_bsky_username` | Your Bluesky handle (e.g., `user.bsky.social`). |
| `roi_bsky_pw` | Your Bluesky App Password. |
| `roi_bsky_post` | Boolean. Globally enable/disable Bluesky posting. |
| `roi_bsky_names` | List of ROI `name` strings that should be posted to Bluesky. |

### ROI Definitions (`rois`)

| Key | Description |
| :--- | :--- |
| `name` | Unique name for the ROI. Can include environment variables (e.g., `${MY_SITE_NAME}`). Spaces are replaced with underscores in filenames but preserved in metadata. |
| `bbox` | WGS84 Bounding Box: `west,south,east,north`. Supports environment variables. |
| `bbox_match` | Minimum coverage percentage (0-100) required to trigger a crop. |
| `products` | List of product suffixes to monitor (e.g., `["TCI", "RATIO", "NDVI", "RADAR-BURN"]`). |
| `apprise_url` | Optional Apprise notification URL (e.g., `pover://...`, `signal://...`). |

> **Note on Environment Variables:** The pipeline supports resolving environment variables in the format `${VAR_NAME}`. The variable must be the entire string for the field (e.g., `"bbox": "${MY_ROI_BBOX}"`).

---

## ROI Retention & Cleanup

By default, ROI crops are preserved even if the raw source data is cleaned up. You can control the retention period in your `.env` file:

```bash
# Keep ROI crops for 90 days (independent of standard tile cleanup)
CLEANUP_ROI_DAYS=90
```

---

## ROI Animator (`roi_animator.py`)

The `roi_animator.py` tool generates ordered PNG sequences from existing ROI crops, facilitating the creation of timelapses or animations.

### Usage

```bash
python roi_animator.py --roi "My_Site" --product TCI --start 2026-01-01 --end 2026-06-01
```

### Arguments

- `--roi`, `-r`: Name of the ROI (must match `roi_config.json`).
- `--product`, `-p`: Product type to animate (e.g., `TCI`, `RATIO`, `NDVI`).
- `--start`, `-s`: (Optional) Start date/time (YYYY-MM-DD or ISO).
- `--end`, `-e`: (Optional) End date/time (YYYY-MM-DD or ISO).
- `--orbit-direction`, `-od`: (Optional) Filter Sentinel-1 products by `ASCENDING` or `DESCENDING`. Crucial for consistent SAR timelapses.
- `--output-dir`, `-o`: (Optional) Override the default animation output directory.

### Output

The tool creates a directory in `output/animations/` containing:
1. A sequence of numbered PNG files (e.g., `00001.png`, `00002.png`).
2. An example `ffmpeg` command for encoding the sequence into a video:

```bash
ffmpeg -framerate 5 -i path/to/sequence/%05d.png -c:v libx264 -pix_fmt yuv420p animation.mp4
```
