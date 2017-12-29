Module bfutils.globus.globus
----------------------------

Classes
-------
GlobusTransfer 
    This class acts as a wrapper utility around the Globus Python CLI.
    This class interacts with the CLI via subprocesses.

    Ancestors (in MRO)
    ------------------
    bfutils.globus.globus.GlobusTransfer

    Instance variables
    ------------------
    destID

    sourceID

    Methods
    -------
    __init__(self, sourceID, destID)

    add_file(self, src_path, dest_path, recursive=False)
        Append a file to self's file list.

        **ARGUMENTS:**  
            *src_path (str)* -- The source path of the file. Must be absolute.  
            *dest_path (str)* -- The destination path of the file. Can be either
                               a directory path or a file path. Must be absolute.  
            *recursive (bool)* -- If src_path is a directory, set recursive to True
                                to transfer entire directory.

    transfer(self, parallel=1, sync_level=None, verify_checksum=False, deadline=None, label=None, retry=False, batch_dir='/mnt/a/u/sciteam/clipp/basicFusion/externLib/BFpyEnv/lib/python2.7/site-packages/bfutils/globus/batch')
        Initiate a Globus transfer request using the files in self's file list.

        **ARGUMENTS:**  
            *parallel (int)* -- Number of concurrent Globus transfer requests to use
                              for self's file list. e.g. parallel=3 will split the 
                              file list 3 way.  
            *sync_level (str)* -- Synchronization level. Set the level at which the 
                                Globus CLI determines whether the files between
                                source and destination differ. Allowable values are:  
                                'exists' will copy if file doesn't exist at dest  
                                'size' will copy if size of file differs  
                                'mtime' will copy if modification times differ  
                                'checksum' will copy if checksums differ  
            *verify_checksum (bool)* -- After transfer, verify that the checksums of
                                      the file between source and dest match. If
                                      they differ, automatically retry file until
                                      this check succeeds.
            *deadline (str)* -- The ISO formatted deadline for the transfer. Can't be larger than
                              30 days from present.  
            *label (str)*    -- The label for the transfer. Defines the name for the batch
                              transfer files and to Globus itself. Non-unique
                              names may cause old batch files to be overwritten.  
            *retry (bool)* -- Retry failed files once.  
            *batch_dir (str)* -- If not none, will save the Globus batch files to
                               this directory.

    wait(self)
        Wait for all of the Globus transfers in self's submitID list to complete.
        ARGUMENTS:
            None
