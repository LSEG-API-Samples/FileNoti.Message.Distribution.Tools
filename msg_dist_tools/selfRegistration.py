#=============================================================================
# Refinitiv Data Platform demo app for Self Registration
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================
import requests
import json
import rdpToken

# Set Application Constants
RDP_version = "/v1"
base_URL = "https://api.refinitiv.com"
category_URL = "/iam/self-registration"
endpoint_URL = "/profile-registration"


#==============================================
if __name__ == "__main__":
#==============================================
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL

	print("Self Registration test")

	# get the latest access token
	accessToken = rdpToken.getToken()
	hdrs = {
		'Authorization': "Bearer " + accessToken,
		'Content-Type': "application/json"
	}

	payload = """
	{
		"esiHeader": {
			"applicationId": "AEM"
		},
		"userProfile": {
			"userInfo": {
				"address": {
					"firstName": "Jackson",
					"lastName": "Francis",
					"userId": "",
					"emailAddress": "jackson.francis@mailinator.com"
				},
				"account": {
					"accountId": "A-00134938"
				},
				"reference": "",
				"parentProductId": "REFINITIV WORKSPACE FOR INVESTMENT BANKERS",
				"products": [
					{
						"productId": "DATASTREAM ADD-ON"
					},
					{
						"productId": ""
					},
					{
						"productId": ""
					}
				],
				"actionCode": "CreateStudent",
				"sourceOrTargetReference": [
					{
						"extensibleAttributes": [
							{
								"systemIdentifier": "RefinitivWorkspace"
							}
						]
					}
				]
			}
		}
	}
	"""

	sResp = requests.post(RESOURCE_ENDPOINT, headers=hdrs, data = payload)
	if sResp.status_code != 200:
		raise ValueError("Unable to self register. Code %s, Message: %s" % (sResp.status_code, sResp.text))
	else:
		print("Request successful, response:")
		jResp = json.loads(sResp.text)
		print(json.dumps(jResp, indent=2))
