import uuid
import os

'''Class that defines a single job in a job-chain workflow'''

class Dependency(object):
    
    _supported_dep = [ 'after', 'afterany', 'afterok', 'afternotok', \
        'before', 'beforeany', 'beforeok', 'beforenotok' ]
    
    def __init__(self, job_0, job_1, type='afterany' ):
        
        if type not in self._supported_dep:
            raise ValueError("Value for argument 'type' is not a supported \
                PBS dependency.")
         
        self.job_0 = job_0
        self.job_1 = job_1
        self.type  = type

    def __eq__( self, other ):
        if not isinstance( self, type(other) ):
            return False

        return self.job_0.get_id() == other.job_0.get_id() \
            and self.job_1.get_id() == other.job_1.get_id()

    def __hash__(self):
        return hash((self.job_0, self.job_1, self.type))

class Job(object):

    def __init__( self, script=None, script_type=None, dependent=None, dep_type='afterany' ):

        # Generate a unique ID for this job
        self._id            = uuid.uuid4()

        # JOB SCRIPT
        #----------------------------------------------------------
        self._job_script    = { 'type' : None, 'script' : None }  #
        self.set_job( script, script_type )                       #
        #----------------------------------------------------------

        # SCHEDULER-PROVIDED VARIABLES
        #--------------------------------------------------------------
        # _sched_id is ID that scheduler itself will provide later on #
        self._sched_id = None                                         #
        #--------------------------------------------------------------

    #====================================================================
    def __hash__(self):
        return hash( self._id )

    #====================================================================
    def set_job( self, script, type = None ):
        '''
**DESCRIPTION**  
    Set the scheduler script to use for this job. Can either pass in a script
    literal or a path to a file. By default, function will attempt to
    interpret argument 'script' as either a literal or an existing file.  
**ARGUMENTS**  
    *script* (str)  -- Script literal or path to a script  
    *type* (str)   -- Set to 'file' or 'literal' to force interpretation of
        script.  
**EFFECTS**
    Stores *script* in self's attributes.  
**RETURN**  
    None
        '''
        supported_types = ['file', 'literal']
        if not isinstance(script, str):
            raise TypeError("Argument 'script' is not a str")

        if type in supported_types:
            self._job_script['type'] = type
        else:

            if os.path.isfile( script ):
                self._job_script['type'] = 'file'
            else:
                self._job_script['type'] = 'literal'

        self._job_script['script'] = script


    #====================================================================
    def get_id( self ):
        return self._id

        
    #====================================================================
    def set_sched_id( self, id ):
        if not isinstance( id, str ):
            raise TypeError("Argument 'id' is not a str.")
        self._sched_id = id

    #====================================================================
    def is_base( dependency ):
        '''
**DESCRIPTION**  
    Checks if self is the base of the dependency (i.e., is dependency's job_0)  
**ARGUMENTS**
    *dependency* (Dependency)   -- The dependency to check if self is the base of  
**EFFECTS**  
    None  
**RETURN**  
    True/False
        '''
        return self == dependency.job_0
