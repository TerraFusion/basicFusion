import globus_sdk
import os.path
import argparse
import time
import sys

BW_ENDPOINT = 'd59900ef-6d04-11e5-ba46-22000b92c6ec'
NL_ENDPOINT = 'd599008e-6d04-11e5-ba46-22000b92c6ec'

# This class encapsulates all of the Globus API calls into one object so that any globus action
# related to transferring data between two enpoints can be performed.

class globusTransfer:
    def __init__( self, sourceEndpoint_a, destEndpoint_a, refreshPath_a= os.path.abspath( os.path.dirname(sys.argv[0])) + '/refresh.txt', \
                  clientIDPath_a= os.path.abspath( os.path.dirname(sys.argv[0])) + '/clientID.txt'):
        self._sourceEndpoint = sourceEndpoint_a
        self._destEndpoint = destEndpoint_a


        # How many concurent Globus calls should be made
        self._parallelism = 1
        # A list of all the files to transfer
        self._dataList = []
        # A list of all the TransferData objects. Each object defines to Globus exactly which files to transfer.
        # A separate object is needed for separate Globus transfer calls
        self._transferData = []
        # What the Globus API returns when we request a data transfer
        self._transferResult = []
        # The Globus Client ID (string)
        self._clientID = ""
         
        self._refreshPath = refreshPath_a
        self._clientIDPath = clientIDPath_a

        # Begin the authorization 
        retAuth = self._authTransferStart()

        # The transfer client where API calls will be made
        self._transClient = retAuth['tc']
        # The authorizer, just in case we need to reauthorize to the API
        self._authorizer = retAuth['authorizer']
    
    # Perform an ls operation
    def ls ( self, endPoint_a, path_a ):    
        for entry in self._transClient.operation_ls( endPoint_a, path = path_a ):
            print( "{} : {}".format( entry["name"], entry["type"]) )

    # Define the maximum number of Globus transfer calls to make
    def parallelism ( self, parallelism_a ):
        self._parallelism = parallelism_a

    # Add an entire directory in the _sourceEndpoint endpoint to the list.
    # sourcePath_a defines what directory in the _sourceEndpoint will be transferred.
    # toPath_a defines what directory in the _toEndpoint these files will be transferred.
    def addDir ( self, sourcePath_a, destPath_a ):

        # Add each file/folder in this directory
        for entry in self._transClient.operation_ls( self._sourceEndpoint, path = sourcePath_a ):
            # For now, just add this to our list. We will figure out how to split each request
            # later.
            self._dataList.append( { "source": sourcePath_a + '/' + entry["name"], "dest": destPath_a + '/' + entry["name"] } )
    

    # Make a directory path_a at endpoint endpoint_a
    def mkdir ( self, endpoint_a, path_a ):
        """     mkdir( self, endpoint_a, path_a ):
        DESCRIPTION:
            This function performs a mkdir operation (make directory) through Globus. The directory to mkdir is given by path_a
            and will be performed at the Globus endpoint endpoint_a. Will print the Globus reponse returned by operation_mkdir.
            Self's _transClient object must first be initialized before this function can be called. This can be done by calling 
            _authTransferStart().
        ARGUMENTS:
            self
            endpoint_a (str)            -- The Globus endpoint to perform mkdir on
            path_a (str)                -- Directory to create
        EFFECTS:
            Creates a directory at the Globus endpoing endpoint_a
        RETURN:
            None
        """
        retVal = self._transClient.operation_mkdir( endpoint_a, path = path_a )
        print( retVal )

    def _authTransferStart( self ):
        """     _authTransferStart(self)
        DESCRIPTION:
            This function handles all of the administrative details of initializing self's authorization and transfer client
            objects. Globus requires a specific set of steps to be completed in order to authorize transfer requests. The user
            may be required to visit some URLs in order to retrieve some authorization tokens for the Globus Python SDK.
        ARGUMENTS:
            self
        EFFECTS:
            Creates two files, self._refreshPath and self._clientIDPath if needed in order to save the refresh token and
            the Globus client ID.
        RETURN:
            tc (TransferClient)         -- The TransferClient object returned by Globus. This is used to submit transfer
                                           requests.
            authorizer (RefreshTokenAuthorizer)         -- The Authorizer object from Globus used to create the transfer
                                                           client.
        """
        if not os.path.isfile( self._clientIDPath ):
            URL="http://globus-sdk-python.readthedocs.io/en/latest/tutorial/"
            print("Please go to this URL and follow steps 1 and 2 to obtain a Client ID: {}".format(URL))
            self._clientID = raw_input("Please enter the Client ID: ")
            self._clientID.strip()

            # Save the Client ID in a file
            with open( self._clientIDPath, "w" ) as f:
                f.write(self._clientID)
        
        else:
            # Open the Client ID file and read it in
            with open ( self._clientIDPath, "r" ) as f:
                self._clientID = f.readline().strip()

        client = globus_sdk.NativeAppAuthClient(self._clientID)

        if not self.refreshIsValid():
            # The refresh token either doesn't exist or it's not valid

            client.oauth2_start_flow(refresh_tokens=True)

            print('Please go to this URL and login: {0}'
                  .format(client.oauth2_get_authorize_url()))

            get_input = getattr(__builtins__, 'raw_input', input)
            auth_code = get_input('Please enter the code here: ').strip()
            token_response = client.oauth2_exchange_code_for_tokens(auth_code)

            # Get the data from the transfer API
            globus_transfer_data = token_response.by_resource_server['transfer.api.globus.org']

            # Get the refresh token
            transfer_rt = globus_transfer_data['refresh_token']
            
            # Save the refresh token
            with open( self._refreshPath, 'w') as f:
                f.write(transfer_rt)
            
            # Get the access token
            transfer_at = globus_transfer_data['access_token']
            # Get the expiration time
            expires_at_s = globus_transfer_data['expires_at_seconds']

            # Now we've got the data we need, but what do we do?
            # That "GlobusAuthorizer" from before is about to come to the rescue

            # Create a refresh authorizer
            authorizer = globus_sdk.RefreshTokenAuthorizer(
                transfer_rt, client, access_token=transfer_at, expires_at=expires_at_s)

            # and try using `tc` to make TransferClient calls. Everything should just
            # work -- for days and days, months and months, even years
            tc = globus_sdk.TransferClient(authorizer=authorizer)

            # Prompt the user if the endpoints need to be activated
            self._autoActivate( self._srcEndpoint, tc )
            self._autoActivate( self._destEndpoint, tc )

        else:
            refreshToken = self._getRefreshToken()
            # authResponse is an authorizer...
            authorizer = globus_sdk.RefreshTokenAuthorizer( refreshToken, client )
            tc = globus_sdk.TransferClient(authorizer=authorizer)

        return {'tc':tc, 'authorizer':authorizer}

    def _autoActivate( self, ep_id ):
        """     _autoActivate( self, ep_id, tc )
        DESCRIPTION:
            This function attempts to auto-activate the Globus endpoint denoted by ep_id. If the auto-activation fails,
            it will prompt the user to go to a URL to activate it online.
        ARGUMENTS:
            self
            ep_id (str)         -- The Globus Endpoint ID to auto-activate
        EFFECTS:
            None
        Return:
            None
        """

        r = self._transClient.endpoint_autoactivate(ep_id, if_expires_in=3600)
        while (r["code"] == "AutoActivationFailed"):
            print("Endpoint requires manual activation, please open "
                  "the following URL in a browser to activate the "
                  "endpoint:")
            print("https://www.globus.org/app/endpoints/{}/activate".format(ep_id) )
            # For python 2.X, use raw_input() instead
            raw_input("Press ENTER after activating the endpoint:")
            r = self._transClient.endpoint_autoactivate(ep_id, if_expires_in=3600)
    
    # After the _dataList has been populated and all of the endpoint information has been esablished,
    # we can go ahead and execute the transfer
    def executeTransfer( self ):
        """     executeTransfer(self)
        DESCRIPTION:
            This function iterates through the self._dataList -- which contains all of the files to transfer -- and
            submits them to Globus for transfer. self._parallelism number of Globus transfers will be submitted, the
            files in self._dataList being split among each transfer. The Globus TransferResponse dictionary, returned
            after calling the Globus submit_transfer function, is saved into the self._transferResult list.
        ARGUMENTS:
            self
        EFFECTS:
            The Globus destination endpoint, defined in self._destEndpoint, will contain all of the files in self._dataList
            if the transfer succeeds.
        RETURN:
            None
        """
        # If the _dataList is empty, throw an error
        assert len(self._dataList) > 0
        assert self._parallelism > 0

        # If our parallelism is greater than the number of entries to be transferred, then
        # cap the parallelism at the number of entries
        if self._parallelism > len( self._dataList ):
            self._parallelism = len( self._dataList )

        # We will have self._parallelism number of TrasferData objects. This is the number of independent Globus
        # transfer calls that will be made. 
        for i in range( 0, self._parallelism ):
            tData = globus_sdk.TransferData( self._transClient, self._sourceEndpoint, self._destEndpoint )
            self._transferData.append( tData )

        # For each Transfer "thread", we want to define which entries in self._dataList each thread will be responsible
        # for transferring.
        #
        # This variable is the truncated number of entries each thread source 1 to ( _parallelism - 1) will handle
        entriesPerThread = len( self._dataList ) / self._parallelism
        # The last thread will handle the remaining number of entries in _dataList. This is just the modulus operator
        numEntriesLast = len( self._dataList ) % self._parallelism

        i = 0
        for j in xrange( 0, len( self._dataList ) ):
            if ( i >= self._parallelism ):
                i = 0
            entry = self._dataList[j]
            # Add this entry to this thread's list of files to transfer
            self._transferData[i].add_item( entry["source"], entry["dest"] )
            # Increment the transfer thread
            i = i + 1
       
        # We now have a list of all the files to transfer ready to pass to the Globus API
        for i in range( 0, self._parallelism ):
            self._transferResult.append( self._transClient.submit_transfer( self._transferData[i] ) )
            print(self._transferResult[i])


    
    def refreshIsValid( self ):
        # TODO: This function is not finished! Need to complete the Globus refresh token validation check!!!
        """     refreshIsValid(self)
        DESCRIPTION:
            This function determines if there is a valid refresh token stored in the file self._refreshPath.
            If this file does not exists, or if the file exists but the token is no longer valid, False will be
            returned.
        ARGUMENTS:
            self
        EFFECTS:
            None
        RETURN:
            True/False
        """
        retVal = True
        
        # If the refresh.txt file doesn't even exist, then refresh token isn't valid
        if not os.path.isfile(self._refreshPath):
            retVal = False
        # Else the file exists, so we then need to use Globus API to check if it's still good
        else:
            try:
                refreshToken = self._getRefreshToken()
            except:
                retVal = False
            #authClient = ConfidentialAppAuthClient( clientID, 'nothing' )
            #data = authClient.oauth2_validate_token('refreshToken')
            #retVal = data['active']
        return retVal

    def _getRefreshToken( self ): 
        """     _getRefreshToken( self )
        DESCRIPTION:
            This function grabs the Globus refresh token from the file self._refreshPath. This file must exist or else an
            exception will occur.
        ARGUMENTS:
            self
        EFFECTS:
            none
        RETURN:
            refreshToken (str)
        """
        # Grab the refresh token
        f = open(self._refreshPath, 'r')
        refreshToken = f.readline()
        f.close()

        return refreshToken

    def transferWait( self ):
        """     transferWait( self )
        DESCRIPTION:
            This function iterates through all of the Globus transfer requests saved in the self._transferResult dictionary list.
            It passes the submission ID of each element in the list to the Globus API and will wait for completion.
        ARGUMENTS:
            self
        EFFECTS:
            None
        RETURN:
            None
        """
        for i in xrange( 0, len( self._transferResult ) ):
            transInfo = self._transferResult[i]
            print("Pausing for Globus transfer task: {}".format( transInfo['task_id'] ) )
            # Iterate through every transfer task that was initiated and wait until they complete.
            # By default, task_wait polls Globus servers every 10 seconds (polling_interval=10). Will
            # timeout after 'timeout' number of seconds. While loop keeps interating until the task is complete.
            # NOTE that for large transfers, this can take a very, very long time!!!
            while not self._transClient.task_wait( transInfo['task_id'], timeout=60 ):
                pass

            print("Transfer task {} completed.".format( transInfo['task_id'] ) )
            
def main():

    parser = argparse.ArgumentParser(description="This script handles transferring a single-level directory through the Globus API. \
                                    This script can only be used to transfer files. It will fail if there are any directories within \
                                    the SRC_DIR directory. Manual user interaction may be needed to log into the Globus servers.")
    parser.add_argument("SRC_ENDPOINT", help="The source Globus Endpoint ID.", type=str )
    parser.add_argument("DEST_ENDPOINT", help="The destination Globus Endpoint ID", type=str )
    parser.add_argument("SRC_DIR", help="The source directory to recursively transfer. NOTE that only the contents of \
                        this directory will be recursively transferred! If you are transferring /path/to/dir, 'dir' itself will not be \
                        transferred but rather all of its children.", type=str)
    parser.add_argument("DEST_DIR", help="The directory on the destination endpoint where the SRC_DIR will be recursively \
                        transferred to.", type=str )
    parser.add_argument("--wait", help="Wait for all of the Globus transfer requests to successfully complete before returning from the \
                        program.", action='store_true')
    parser.add_argument("--parallelism", help="Define how many Globus transfer requests to submit at one time. Note that by default, \
                        Globus will not allow more than 3 active transfers at once. This value defaults to 3.", type=int, default=3 )
    parser.add_argument("--check-login", dest="checkLogin", \
                        help="Use this option to ensure that all Globus credentials are in order. This step is implicitly performed \
                        any time the script is run, but explicitly giving this command forces the script to ONLY run the log-in checks. \
                        It may be the case that no login actions will need to be performed. Note that valid values must still be supplied \
                        for the positional arguments SRC_ENDPOINT and DEST_ENDPOINT, but the SRC and DEST_DIR may be junk values.", \
                        action='store_true' )

    args = parser.parse_args()

    transfer = globusTransfer( args.SRC_ENDPOINT, args.DEST_ENDPOINT )
    if not args.checkLogin:
        # Define the parallelism
        transfer.parallelism( args.parallelism )
        transfer.addDir( args.SRC_DIR, args.DEST_DIR)
        #transfer.mkdir( BW_ENDPOINT, '/scratch/sciteam/clipp/BFoutput/2014' )
        transfer.executeTransfer()
     
        if args.wait:
           transfer.transferWait()
    else:
        print("Globus login checks complete.")
 
if __name__ == '__main__':
    main()
