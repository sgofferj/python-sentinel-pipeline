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
import time
from typing import Optional

import apprise

import functions as func


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
        func.perf_logger.log_info("Notification skipped: no Apprise URLs configured")
        return

    apobj = apprise.Apprise()

    # Split by comma or space if multiple URLs are provided
    for url in urls.replace(",", " ").split():
        if url.strip():
            apobj.add(url.strip())

    if len(apobj) > 0:
        # Set a longer timeout on self-hosted plugin types: message
        # delivery can take >4s (Apprise default is 4s), and the daemon
        # may briefly drop the connection and needs time to reconnect.
        for plugin in apobj:
            try:
                if isinstance(plugin, apprise.plugins.signal_api.NotifySignalAPI):
                    plugin.socket_connect_timeout = 60.0
                    plugin.socket_read_timeout = 60.0
            except (ImportError, AttributeError):
                pass  # plugin type not installed, skip

        print(f"Sending notification to {len(apobj)} targets...", flush=True)
        func.perf_logger.log_info(
            f"Sending notification to {len(apobj)} targets"
        )

        attach = None
        if attachment and os.path.exists(attachment):
            # AppriseAttachment can take a file path
            attach = apprise.AppriseAttachment(attachment)
        elif attachment:
            msg = f"Attachment not found for notification: {attachment}"
            print(msg, flush=True)
            func.perf_logger.log_info(msg)

        try:
            success = apobj.notify(
                body=message,
                title=title,
                attach=attach,
            )
        except Exception as e:
            msg = f"Notification raised exception: {e}"
            print(msg, flush=True)
            func.perf_logger.log_info(msg)
            success = False

        # Retry once if failed (covers transient daemon disconnect/reconnect)
        if not success:
            print("Notification failed, retrying in 10s...", flush=True)
            func.perf_logger.log_info("Notification failed, retrying in 10s")
            time.sleep(10)
            try:
                success = apobj.notify(
                    body=message,
                    title=title,
                    attach=attach,
                )
            except Exception as e:
                msg = f"Notification retry raised exception: {e}"
                print(msg, flush=True)
                func.perf_logger.log_info(msg)
                success = False

        if success:
            print("Notification sent successfully.", flush=True)
            func.perf_logger.log_info("Notification sent successfully")
        else:
            print("Notification FAILED — check Apprise URL or server connectivity.", flush=True)
            func.perf_logger.log_info("Notification FAILED")
    else:
        print("Warning: No valid Apprise URLs found.", flush=True)
        func.perf_logger.log_info("No valid Apprise URLs could be parsed from configuration")
