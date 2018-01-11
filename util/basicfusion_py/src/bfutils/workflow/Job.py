import uuid
import os
import subprocess
from collections import defaultdict
import uuid
from bfutils.workflow import constants
import errno
import time

'''Class that defines a single job in a job-chain workflow'''

class Dependency(object):
    
    _supported_dep = [ 'after', 'afterany', 'afterok', 'afternotok', \
        'before', 'beforeany', 'beforeok', 'beforenotok' ]
    
    def __init__(self, base, target, type='afterany' ):
        
        if type not in self._supported_dep:
            raise ValueError("Value for argument 'type' is not a supported \
                PBS dependency.")
        
        if not isinstance( base, Job ):
            raise TypeError("Argument 'base' is not of type Job")
        if not isinstance( target, Job ):
            raise TypeError("Argument 'target' is not of type Job")
 
        self._base      = base
        self._target    = target
        self._type      = type

    def __eq__( self, other ):
        if not isinstance( self, type(other) ):
            return False

        return self._base.get_id() == other._base.get_id() \
            and self._target.get_id() == other._target.get_id()

    def __hash__(self):
        return hash((self._base, self._target, self._type))
    
    def get_base(self):
        '''
**DESCRIPTION**  
    Get the base of the dependency.  
**ARGUMENTS**  
    None  
**EFFECTS**  
    None  
**RETURN**  
    Job object
        '''
        return self._base

    def get_target(self):
        '''
**DESCRIPTION**  
    Get the target of the dependency.  
**ARGUMENTS**  
    None  
**EFFECTS**  
    None  
**RETURN**  
    Job object
        '''
        return self._target

    def get_type(self):
        '''
**DESCRIPTION**  
    Get the type of the dependency. Valid values are defined by the system
    job scheduler.  
**ARGUMENTS**  
    None  
**EFFECTS**  
    None  
**RETURN**  
    str
        '''
        return self._type

class Job(object):
    
    _valid_sched = { 'pbs' : 'qsub' }
    '''Scheduler name to executable mappings'''

    def __init__( self, script, script_type=None ):

        # Generate a unique ID for this job
        self._id            = uuid.uuid4()

        # JOB SCRIPT
        #----------------------------------------------------------
        self._job_script    = { 'type' : None, 'script' : None }  #
        self.set_script( script, script_type )                    #
        #----------------------------------------------------------

        # SCHEDULER VARIABLES
        #---------------------------------------------------------------
        # _sched_id is ID that scheduler itself will provide later on  #
        self._sched_id          = None                                 #
        self._sched_type        = None                                 #
        # override_sched changes behavior of sched_type interpretation #
        self._sched_override    = False                                #
        #---------------------------------------------------------------

    #====================================================================
    def __hash__(self):
        return hash( self._id )

    #====================================================================
    def set_script( self, script, type = None ):
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
        '''
**DESCRIPTION**  
    Get the internal uuid identifier of self.  
**ARGUMENTS**  
    None  
**EFFECTS**  
    None  
**RETURN**  
    uuid object
        '''
        return self._id
        
    #====================================================================
    def set_sched_id( self, id ):
        '''
**DESCRIPTION**  
    Set self's identifier given by the system's scheduler.  
**ARGUMENTS**  
    *id* (str)  -- Identifier understood by the system scheduler.  
**EFFECTS** 
    Sets self's scheduler ID attribute.  
**RETURN**  
    None
        '''
        if not isinstance( id, str ):
            raise TypeError("Argument 'id' is not a str.")
        self._sched_id = id

    #====================================================================
    def get_sched_id( self ):
        '''
**DESCRIPTION**  
    Return self's ID as given by the system scheduler. Attribute will not
    contain a valid value until self is submitted to the scheduler and
    assigned a proper ID.  
**ARGUMENTS**  
    None  
**EFFECTS**  
    None  
**RETURN**  
    Scheduler ID (str)
        '''
        return self._sched_id

    #====================================================================
    def set_sched( self, type, override=False ):
        '''
**DESCRIPTION**  
    Set the system scheduler type. Typical schedulers may be: pbs, slurm, moab.  
**ARGUMENTS**  
    *type* (str)    -- Type of scheduler to use. Supported values: pbs  
    *override* (bool)   -- If true, will use string passed in type as a
        command line literal. i.e. *type* must be an actual terminal command
        used for submitting jobs. If false, type will be internally mapped to
        valid scheduler command-line semantics.  
**EFFECTS**  
    Modifies private attribute.  
**RETURN**  
    None
        '''


        if not override and type not in self._valid_sched:
            raise ValueError('Invalid scheduler type.')

        self._sched_type = type
        self._sched_override = override
        
    #====================================================================
    def is_base( self, dependency ):
        '''
**DESCRIPTION**  
    Checks if self is the base of the dependency  
**ARGUMENTS**  
    *dependency* (Dependency)   -- The dependency to check if self is the base of  
**EFFECTS**  
    None  
**RETURN**  
    True/False
        '''
        return self == dependency.job_0

    def submit( self, dependency = None, params = None ):
        '''
**DESCRIPTION**  
    Submits the job to the system job scheduler.  
**ARGUMENTS**  
    *dependency* (list of Dependency) -- Specifies all job dependencies.  
    *params* (str) -- Extra command line arguments to add to the scheduler
        call.  
**EFFECTS**  
    Submits job to the queue.  
**RETURN**  
    None
        '''

        # Perform automatic garbage collection of old PBS files
        # TODO implement this garbage collection

        dep = defaultdict(list)
        # Gather all the dependencies into a nice dict, perform sanity checks
        for i in dependency:
            if self is not i.get_base():
                raise RuntimeError("Listed a dependency in which self is not the base!")
            dep[ i.get_type() ].append( i.get_target() )

        if self._job_script['type'] == 'literal':
            # Assign a unique name to PBS file and write the string literal 
            # to it
            pbs_name = '{}.pbs'.format( self._id )
            full_path = os.path.join( constants.PBS_DIR, pbs_name )
            
            try:
                os.makedirs( constants.PBS_DIR )
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
            
            with open( full_path, 'w' ) as f:
                f.write( self._job_script['script'] )

        # Create the command line arguments
        args = []
        # append executable name
        if self._sched_override:
            args.append( self._sched_type )
        else:
            args.append( self._valid_sched[ self._sched_type ] )

        # Add PBS dependencies
        for dep_type in dep:
            args.append( '-W' )
            dep_string = 'depend={}'.format( dep_type )
            for job in dep[dep_type]:
                sched_id = job.get_sched_id()

                if sched_id is None:
                    raise ValueError(\
                    'Job {} has not been submitted to the scheduler!'\
                    .format( job ) )

                dep_string += ':{}'.format( sched_id )

            print dep_string

        # subproc = subprocess.Popen(
