# ToDo

## Viewer multilanguage

Make the viewer multilanguage. Start with Finnish, Swedish, English and German.

## Add bluesky autoposter for ROI products

- Use [https://github.com/dmoggles/blueskysocial] unless you know a better option (Note: Apprise doesn't seem to support image posting)
- Add to roi_config.json:

```json
"config":{"roi_bsky_post":(bool), "roi_bsky_names":["list of ROI names to post"], "roi_bsky_username":"string", "roi_bsky_pw":"string"}
```

- Move exisiting ROI config into sub-element "rois"
- Add "apprise_url" string object stub to ROI entries for future use (still working on the concept)
- Loop through roi_bsky_names
- Create non-georeferenced JPEG from TIF. If width or height >4000px, downscale to 4000px while keeping aspect ratio. If JPEG output size >2MB, use HEIC.
- Create one Bluesky post per ROI
- Don't force ROI names from ${} .env-variables to lowercase
- Replace _ in ROI names with space
- Post content:

```text
  Updated {product type} image of {ROI name}
  Satellite: {constellation name}
  Acquisition time: {acquisition time}
  Made with material from Copernicus Sentinel
```

- Include image in post
