#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# notifications.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

"""
Notification module for the Sentinel pipeline using Apprise.
"""

import os
import apprise

def send_notification(message: str, title: str = "Sentinel Pipeline") -> None:
    """Sends a notification via Apprise if APPRISE_URLS is set."""
    urls = os.getenv("APPRISE_URLS")
    if not urls:
        return

    apobj = apprise.Apprise()
    
    # Split by comma or space if multiple URLs are provided
    for url in urls.replace(',', ' ').split():
        if url.strip():
            apobj.add(url.strip())

    if len(apobj) > 0:
        print(f"Sending notification to {len(apobj)} targets...", flush=True)
        apobj.notify(
            body=message,
            title=title,
        )
    else:
        print("Warning: APPRISE_URLS set but no valid URLs found.", flush=True)
