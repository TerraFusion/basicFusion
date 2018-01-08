from __future__ import print_function
from bfutils.workflow import Job
import subprocess
import uuid
from collections import defaultdict
from Job import Job
from Job import Dependency

__author__ = 'Landon T. Clipp'
__email__  = 'clipp2@illinois.edu'
    
class JobChain(object):
    '''Class that manages a chain of Jobs'''

    
    def __init__( self, sched_type = None, **kwargs ):
        # Set the user-defined kwargs as attributes. Nothing in kwargs
        # should be used by the class itself. Only for user convenience.
        for key, value in kwargs.items():
            setattr( self, key, value )

        # SCHEDULER DEFINITION
        # Define the system's scheduler executable. For PBS, this is 'qsub'.
        #----------------------------

        # TODO TODO TODO
        # Change sched_type behavior so that user can't specify actual executable,
        # only the class of scheduler to use. This is to heighten security.
        self._sched_type = None #
        set_sched( sched_type )
        #----------------------------
       
        # ADJACENCY LIST
        #------------------------------------
        self._graph  = defaultdict(list)    #
        #------------------------------------
   
        self._num_vert = 0
 
    #====================================================================
    def set_sched( self, sched_cmd, check_valid = True ):
        '''
**DESCRIPTION**  
    Set the command line job scheduler executable that this class will
    use for submitting jobs.  
**ARGUMENTS**  
    *sched_cmd* (str)    -- Command line executable to submit jobs to  
    *check_valid (Bool) -- Perform check to see if executable is currently
        visible.  
**EFFECTS**  
    Sets self's scheduler command attribute.  
**RETURN**  
    None  
        '''
        if type is None:
            return

        if check_valid:
            retval = subprocess.call( ['which', sched_cmd] )
            if retval != 0:
                raise RuntimeError('Could not find the {} executable. \
                    Set check_valid = False to disable this check.'\
                    .format( sched_cmd ))

        self._sched_cmd = sched_cmd

    #====================================================================   
    
    def add_job( self, job ):
        '''
**DESCRIPTION**
    Define the job to add to the workflow chain.  
**ARGUMENTS**  
    *job* (Job) -- Job definition. Must be a Job object.  
**EFFECTS**  
    Appends job to self's adjacency list. Overwrites adjacency entry if job
    already exists.  
**RETURN**  
    None
        '''

        if not isinstance(job, Job):
            raise TypeError("Argument 'job' is not of type Job.")
       
        job.num = self._num_vert 
        self._graph[ job ] = []

        self._num_vert += 1

    #====================================================================
    def set_dep( self, job_0, job_1, type ):
        '''
**DESCRIPTION**:  
    Set a dependency between two jobs.  
**ARGUMENTS**:  
    *job_0* (Job)   -- A base Job object
    *job_1* (Job)   -- The job that job_0 is dependent on
    *type* (str)    -- The type of dependency. See Dependency documentation
                       for list of appropriatae types.  
**EFFECTS**:  
    Creates new Dependency object.
**RETURN**:
    None
        '''
    
        # job_0 before job_1
        # is the equivalent to
        # job_1 after job_0
        # I force the 'before' prefix because it makes topological sorting 
        # of the graph easier and more efficient by making edges in the list
        # imply a forward traversal in time.
        # e.g.
        # 0 -> 1, 2
        # 2 -> 3
        # Means that 1 and 2 come after 0, and 3 comes after 2. Thus, 0
        # must be BEFORE 1 and 2, etc.
        if 'before' in type:
            type = type.replace('before', 'after')
            tmp = job_0
            job_0 = job_1
            job_1 = tmp
        
        # Discard Dependency if it already exists.
        new_dep = Dependency( job_0, job_1, type )
        try:    
            self._graph[ job_0 ].remove( new_dep )
        except ValueError:
            pass

        # Add the dependency
        self._graph[ job_0 ].append( new_dep )


    #====================================================================   
    def _topo_sort_util( self, vert, visited, stack ):
        '''
**DESCRIPTION**  
    This function is a topological utility function that performs a
    depth first search recursively, inserting the current vertex after
    the search.  
**ARGUMENTS**
    *vert* (Job)    -- The base vertex to search from  
    *visited* (dict of Bool)    -- Dictionary with Job objects as keys where 
        the value indicates if the Job/vertex has been visited.  
    *stack* (list of Job)       -- List that will contain the sorted objects.  
**EFFECTS**  
    Updates stack with sorted Jobs  
**RETURN**  
    None
        '''
        if visited[vert] == 'grey':
            print( vert.num )
            raise RuntimeError("Cycle detected in the job dependency graph!")
            
        elif visited[vert] == 'black':
            return
            
        visited[vert] = 'grey'
        
        print( 'VERT: {}'.format(vert.num) )
        # For all of its neighbors, do a recursive search
        for i in self._graph[vert]:
            if not visited[i.job_1]:
                print('DEP {}'.format( i.job_1.num) )
            self._topo_sort_util( i.job_1, visited, stack )
                 
        # Only after all dependencies have been searched, we will insert the stack
        stack.insert(0, vert)
        visited[ vert ] = 'black'

    #====================================================================   
    def topo_sort(self):
        '''
**DESCRIPTION**  
    Sort self's internal graph topologically. Return the sorted vertices.  
**ARGUMENTS**  
    None  
**EFFECTS**  
    None  
**RETURN**  
    List of Job objects sorted topologically.  
        '''
        stack = []
        visited = {}

        # Semantics for visited dict:
        # white -- node unvisited
        # grey  -- node currently having subtree searched
        # black -- node exited

        for job in self._graph:
            visited[ job ] = False
        
        for i in self._graph:
            self._topo_sort_util( i, visited, stack )
            

        stack.reverse()
        return stack

    #====================================================================   
    def submit( self ):
        
        sort_jobs = self.topo_sort()

        
#        for i in self._graph:
#            print( '{} '.format(i.num), end='' )
#            for j in self._graph[ i ]:
#                print( '{} '.format(j.job_1.num), end='' )
#            
#            print('---')
#        job_order = self.topo_sort()
#
#        print("AFTER")
#        for i in job_order:
#            print( '{} '.format(i.num), end='' )
#            for j in self._graph[ i ]:
#                print( '{} '.format(j.job_1.num), end='' )
#            
#            print('---')

        
