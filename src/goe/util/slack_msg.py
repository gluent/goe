#! /usr/bin/env python3

# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Slack 'simple messaging' module
"""

import json
import logging
import requests


###############################################################################
# CONSTANTS
###############################################################################

DEFAULT_WEBHOOK = (
    "https://hooks.slack.com/services/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"
)
DEFAULT_USER = "linuxbot"
DEFAULT_ICON = "robot_face"


###############################################################################
# EXCEPTIONS
###############################################################################
class SlackMessengerException(Exception):
    pass


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


###########################################################################
# SlackMessenger class
###########################################################################


class SlackMessenger(object):
    """SlackMessage: Send messages to team's slack"""

    def __init__(self, webhook=DEFAULT_WEBHOOK):
        """CONSTRUCTOR"""

        self._webhook = webhook  # Slack 'incoming messages' webhook

        self._init_request()

        logger.debug("SlackMessenger() object successfully initialized")

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _init_request(self):
        """Initialize variables for request"""

        self._status = None
        self._error = None

    def _send(self, channel, message, user, icon):
        """Send a message to slack channel and parse response for success

        Returns True if send was successful, False otherwise
        """
        success = False

        send_data = {
            "channel": channel,
            "username": user,
            "text": message,
            "icon_emoji": icon,
        }

        logger.debug("Sending: %s to slack" % send_data)

        try:
            response = requests.post(self._webhook, data=json.dumps(send_data))
            self._status = response.status_code

            # Check if the status was successful
            if response.status_code >= 200 and response.status_code < 300:
                success = True
            else:
                success = False
            logger.debug(
                "Sending: %s to slack. Response code: %d. Status: %s"
                % (send_data, response.status_code, success)
            )
        except requests.exceptions.RequestException as e:
            self._status, self._error = -1, e
            logger.warn("EXCEPTION: %s while sending: %s to slack" % (e, send_data))
            success = False

        return success

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def status(self):
        """HTTP status code or -1 if exception"""
        return self._status

    @property
    def error(self):
        """Error message if exception"""
        return self._error

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def send(self, channel, message, user=DEFAULT_USER, icon=DEFAULT_ICON):
        """Send slack 'message':
            - to specified 'channel' (channel='#channel_name') or user: (channel='@username')
            - from 'user': (name = 'user', icon emoji = :'icon':)

        i.e. slack.send('#integration-tests', 'Heya!', 'maxym', 'unicornface')

        Returns True if successful, False otherwise
        """
        assert channel and message and user and icon

        if channel[0] not in ("#", "@"):
            raise SlackMessengerException(
                "'Channel' name must start with either: '#' or '@'"
            )
        if not icon.startswith(":"):
            icon = ":" + icon
        if not icon.endswith(":"):
            icon = icon + ":"

        self._init_request()

        return self._send(channel, message, user, icon)


if __name__ == "__main__":
    import sys
    from termcolor import cprint

    def usage(prog_name):
        print("usage: %s <channel> <message> [user [emoji_icon]]" % prog_name)
        sys.exit(1)

    if len(sys.argv) < 3:
        usage(sys.argv[0])

    channel, message = sys.argv[1:3]
    user = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_USER
    icon = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_ICON

    slack = SlackMessenger(webhook=DEFAULT_WEBHOOK)
    if slack.send(channel, message, user, icon):
        cprint("Success", color="green")
        sys.exit(0)
    else:
        cprint("Error: %s" % slack.error, color="red")
        sys.exit(-1)
