#!/bin/bash

IMAGERY="/media/sgofferj/67f5b0d8-60ec-4880-bfc9-ea9ebdadeda9/Sat/output"
# Fallback for new UUID with trailing 1 (handle both mount variants)
if [ ! -d "$IMAGERY" ]; then
    ALT="/media/sgofferj/67f5b0d8-60ec-4880-bfc9-ea9ebdadeda91/Sat/output"
    if [ -d "$ALT" ]; then IMAGERY="$ALT"; fi
fi

# --- Safeguard: abort if drive not mounted or imagery empty (prevents wiping remote with --delete) ---
if ! mountpoint -q "$(dirname "$IMAGERY")" && ! mountpoint -q "$IMAGERY" && ! findmnt -n -S UUID=67f5b0d8-60ec-4880-bfc9-ea9ebdadeda91 >/dev/null 2>&1 && ! findmnt -n -S UUID=67f5b0d8-60ec-4880-bfc9-ea9ebdadeda9 >/dev/null 2>&1; then
    echo "ERROR: Sat drive not mounted (no mountpoint for $IMAGERY) — aborting sync to avoid wiping remote" >&2
    exit 1
fi
if [ ! -d "$IMAGERY/visual" ] || [ -z "$(ls -A "$IMAGERY/visual" 2>/dev/null)" ]; then
    echo "ERROR: $IMAGERY/visual is empty or missing — aborting sync to avoid wiping remote with --delete" >&2
    exit 1
fi
if [ ! -d "$IMAGERY/legends" ] || [ ! -f "$IMAGERY/legends/legends.json" ]; then
    echo "WARNING: $IMAGERY/legends missing — will skip legends sync" >&2
fi

rsync -av --delete -e "ssh -T -o Compression=no -x" ${IMAGERY}/visual/ nostromo:/storage/docker/dev-web/satpipeline/imagery/visual/
rsync -av --delete -e "ssh -T -o Compression=no -x" ${IMAGERY}/legends/ nostromo:/storage/docker/dev-web/satpipeline/imagery/legends/
rsync -av --delete --exclude "logo.png" --exclude "imagery" --exclude "overlays/*" -e "ssh -T -o Compression=no -x" viewer/ nostromo:/storage/docker/dev-web/satpipeline/viewer/
