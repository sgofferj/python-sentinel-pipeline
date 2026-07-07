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
from typing import Optional

import apprise


def send_notification(
    message: str,
    title: str = "Sentinel Pipeline",
    urls: Optional[str] = None,
    attachment: Optional[str] = None,
) -> None:
    """Sends a notification via Apprise."""
    if not urls:
        urls = os.getenv("APPRISE_URLS")

    if not urls:
        return

    apobj = apprise.Apprise()

    # Split by comma or space if multiple URLs are provided
    for url in urls.replace(",", " ").split():
        if url.strip():
            apobj.add(url.strip())

    if len(apobj) > 0:
        print(f"Sending notification to {len(apobj)} targets...", flush=True)

        attach = None
        if attachment and os.path.exists(attachment):
            # AppriseAttachment can take a file path
            attach = apprise.AppriseAttachment(attachment)

        apobj.notify(
            body=message,
            title=title,
            attach=attach,
        )
    else:
        print("Warning: No valid Apprise URLs found.", flush=True)
