#=============================================================================
# Refinitiv Data Platform demo app to change OAuth password
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================
import sys
import rdpToken


#==============================================
if __name__ == "__main__":	
#==============================================
	if len(sys.argv) < 5:
		print("Please provide these four arguments: USERNAME, OLD_PASSWORD, CLIENT_ID, NEW_PASSWORD")
	else:
		# Change the password
		print("Changing the password and getting a new OAuth access token...")
		rdpToken.changePassword(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
		print("Finished")


