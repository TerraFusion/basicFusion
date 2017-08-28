import globus_sdk
import os.path

CLIENT_ID = '7d657ac4-94fe-436e-b7bd-caad66082b5f'
BW_ENDPOINT = 'd59900ef-6d04-11e5-ba46-22000b92c6ec'
BW_PATH = '/scratch/sciteam/clipp/TERRA_BF_L1B_O74607_20131227133440_F000_V000.h5'
NL_ENDPOINT = 'd599008e-6d04-11e5-ba46-22000b92c6ec'
NL_PATH = '/projects/sciteam/bage/BFoutput/2013/TERRA_BF_L1B_O74607_20131227133440_F000_V000.h5'
RF_TOKEN_PATH = './refresh.txt'

# This class encapsulates all of the Globus API calls into one object so that any globus action
# related to transferring data between two enpoints can be performed.

class globusTransfer:
    def __init__( self, sourceEndpoint_a, destEndpoint_a):
        self._sourceEndpoint = sourceEndpoint_a
        self._destEndpoint = destEndpoint_a

        # Begin the authorization 
        retAuth = authTransferStart()

        # The transfer client where API calls will be made
        self._transClient = retAuth['tc']
        # The authorizer, just in case we need to reauthorize to the API
        self._authorizer = retAuth['authorizer']

        # How many concurent Globus calls should be made
        self._parallelism = 1
        # A list of all the files to transfer
        self._dataList = []
        # A list of all the TransferData objects. Each object defines to Globus exactly which files to transfer.
        # A separate object is needed for separate Globus transfer calls
        self._transferData = []
        # What the Globus API returns when we request a data transfer
        self._transferResult = []

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
        retVal = self._transClient.operation_mkdir( endpoint_a, path = path_a )
        print( retVal )

    # After the _dataList has been populated and all of the endpoint information has been esablished,
    # we can go ahead and execute the transfer
    def executeTransfer( self ):
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

        j = 0

        if self._parallelism == 1:
            for i in range ( 0, entriesPerThread + numEntriesLast ):
                entry = self._dataList[j]
                self._transferData[i].add_item( entry["source"], entry["dest"] )
                # Increment the dataList index
                j = j + 1

        else:
            for i in range( 0 , self._parallelism ):
                if i < self._parallelism - 1:
                    for w in range ( 0, entriesPerThread ):
                        # Grab the entry source the data list
                        entry = self._dataList[j]
                        # Add this entry to this thread's list of files to transfer
                        self._transferData[i].add_item( entry["source"], entry["dest"] )
                        # Increment the dataList index
                        j = j + 1
                elif i == self._parallelism - 1: 
                    # The last thread...
                    for w in range ( 0, numEntriesLast ):
                        entry = self._dataList[j]
                        self._transferData[ i ].add_item( entry["source"], entry["dest"] )
                        # Increment the dataList index
                        j = j + 1

       
        # We now have a list of all the files to transfer ready to pass to the Globus API
        for i in range( 0, self._parallelism ):
            self._transferResult.append( self._transClient.submit_transfer( self._transferData[i] ) )
            print(self._transferResult[i])


def refreshIsValid():
    retVal = True
    
    # If the refresh.txt file doesn't even exist, then refresh token isn't valid
    if not os.path.isfile(RF_TOKEN_PATH):
        retVal = False
    # Else the file exists, so we then need to use Globus API to check if it's still good
    else:
        refreshToken = getRefreshToken()
        #authClient = ConfidentialAppAuthClient( CLIENT_ID, 'nothing' )
        #data = authClient.oauth2_validate_token('refreshToken')
        #retVal = data['active']
        retVal = True
    return retVal

def getRefreshToken(): 
    # Grab the refresh token
    f = open(RF_TOKEN_PATH, 'r')
    refreshToken = f.readline()
    f.close()

    return refreshToken

def authTransferStart():

    client = globus_sdk.NativeAppAuthClient(CLIENT_ID)

    if not refreshIsValid():
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
        with open(RF_TOKEN_PATH, 'w') as f:
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

    else:
        refreshToken = getRefreshToken()
        # authResponse is an authorizer...
        authorizer = globus_sdk.RefreshTokenAuthorizer( refreshToken, client )
        tc = globus_sdk.TransferClient(authorizer=authorizer)

    return {'tc':tc, 'authorizer':authorizer}

def main():
   
    transfer = globusTransfer( NL_ENDPOINT, BW_ENDPOINT )
    # Define the parallelism
    transfer.parallelism( 10 )
    transfer.addDir( '/projects/sciteam/bage/BFoutput/2013', '/scratch/sciteam/clipp/BFoutput')
    #transfer.mkdir( BW_ENDPOINT, '/scratch/sciteam/clipp/BFoutput/2014' )
    transfer.executeTransfer()
 
if __name__ == '__main__':
    main()
