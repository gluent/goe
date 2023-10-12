from pdb import post_mortem
import requests
import json

# The client would pass the API-KEY in the headers
headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer SECRET-API-KEY'
}
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
