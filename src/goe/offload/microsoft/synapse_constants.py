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

# Authentication mechanisms
SYNAPSE_AUTH_MECHANISM_SQL_PASSWORD = "SqlPassword"
SYNAPSE_AUTH_MECHANISM_AD_PASSWORD = "ActiveDirectoryPassword"
SYNAPSE_AUTH_MECHANISM_AD_MSI = "ActiveDirectoryMsi"
SYNAPSE_AUTH_MECHANISM_AD_SERVICE_PRINCIPAL = "ActiveDirectoryServicePrincipal"
SYNAPSE_VALID_AUTH_MECHANISMS = [
    SYNAPSE_AUTH_MECHANISM_AD_PASSWORD,
    SYNAPSE_AUTH_MECHANISM_AD_MSI,
    SYNAPSE_AUTH_MECHANISM_AD_SERVICE_PRINCIPAL,
    SYNAPSE_AUTH_MECHANISM_SQL_PASSWORD,
]
SYNAPSE_USER_PASS_AUTH_MECHANISMS = [
    SYNAPSE_AUTH_MECHANISM_AD_PASSWORD,
    SYNAPSE_AUTH_MECHANISM_SQL_PASSWORD,
]
