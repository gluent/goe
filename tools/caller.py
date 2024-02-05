# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pdb import post_mortem
import requests
import json

# The client would pass the API-KEY in the headers
headers = {"Content-Type": "application/json", "Authorization": "Bearer SECRET-API-KEY"}
return_payload = {}

# data = {"arg1": "First Argument", "arg2": "Second Argument"}
# post_url = "http://127.0.0.1:8080/offload-table/test/reset-table/true"
# post_respose =  requests.post(post_url, headers=headers, json=data)

# New Api path
get_url = "http://127.0.0.1:8000/api/metadata/version"
get_response = requests.get(get_url, headers=headers)
print(get_response.text)

# # Return Offload Home
# get_url = 'http://127.0.0.1:8080/get-offload-home'
# response = requests.get(get_url, headers=headers, data=return_payload)
# print(response.text)

# # Return Offload Version
# get_url = 'http://127.0.0.1:8080/get-offload-version'
# response = requests.get(get_url, headers=headers, data=return_payload.clear())
# print(response.text)
