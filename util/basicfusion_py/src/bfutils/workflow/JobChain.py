from __future__ import print_function
from bfutils.workflow import Job
import subprocess
import uuid
from collections import defaultdict
from Job import Job
from Job import Dependency
import os

__author__ = 'Landon T. Clipp'
__email__  = 'clipp2@illinois.edu'
    
class JobChain(object):
    '''Class that manages a chain of Jobs'''

    
    def __init__( self, sched_type, **kwargs ):
        # Set the user-defined kwargs as attributes. Nothing in kwargs
        # should be used by the class itself. Only for user convenience.
        for key, value in kwargs.items():
            setattr( self, key, value )

        # SCHEDULER DEFINITION
        # Define the system's scheduler executable. For PBS, this is 'qsub'.
        #----------------------------
        self._sched_type = None #
        self.set_sched( sched_type )
        #----------------------------
       
        # ADJACENCY LIST
        #------------------------------------
        self._graph  = defaultdict(list)    #
        #------------------------------------
   
        self._num_vert = 0
 
    #====================================================================
    def set_sched( self, sched_type):
        '''
**DESCRIPTION**  
    Set the job scheduler type.  
**ARGUMENTS**  
    *sched_type* (str)    -- Command line executable to submit jobs to  
**EFFECTS**  
    Sets self's scheduler command attribute.  
**RETURN**  
    None  
        '''
        if type is None:
            return


        self._sched_type = sched_type

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
    def set_dep( self, base, target, type ):
        '''
**DESCRIPTION**:  
    Set a dependency between two jobs.  
**ARGUMENTS**:  
    *base* (Job)   -- A base Job object
    *target* (Job)   -- The job that *base* is dependent on
    *type* (str)    -- The type of dependency. See Dependency documentation
                       for list of appropriatae types.  
**EFFECTS**:  
    Creates new Dependency object.
**RETURN**:
    None
        '''
    
        # base before target
        # is the equivalent to
        # target after base
        # I force the 'after' prefix because it makes topological sorting 
        # of the graph easier and more efficient by making edges in the list
        # imply a forward traversal in time.
        # e.g.
        # 0 -> 1, 2
        # 2 -> 3
        # Means that 1 and 2 come after 0, and 3 comes after 2. Thus, 0
        # must be BEFORE 1 and 2, and 2 is BEFORE 3, etc.
        if 'before' in type:
            type = type.replace('before', 'after')
            tmp = base
            base = target
            target = tmp
        
        # Discard Dependency if it already exists.
        new_dep = Dependency( base, target, type )
        try:    
            self._graph[ base ].remove( new_dep )
        except ValueError:
            pass

        # Add the dependency
        self._graph[ base ].append( new_dep )


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
            raise RuntimeError("Cycle detected in the job dependency graph!")
            
        elif visited[vert] == 'black':
            return
            
        visited[vert] = 'grey'
        
        # For all of its neighbors, do a recursive search
        for i in self._graph[vert]:
            self._topo_sort_util( i.get_target(), visited, stack )
                 
        # Only after all neighbors have been searched, we will insert the stack
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
            visited[ job ] = 'white'
        
        for i in self._graph:
            self._topo_sort_util( i, visited, stack )
            

        stack.reverse()
        return stack

    #====================================================================   
    def submit( self, print_map=False ):
        '''
**DESCRIPTION**  
    Submits all Jobs added by add_job() to the system scheduler.  
**ARGUMENTS**  
    *print_map* (bool)  -- Return a string representation of the job
        dependency map.
**EFFECTS**  
    Submits jobs to the scheduler.  
**RETURN**  
    If print_map == True: string  
    If print_map == False: None
        '''    
        sort_jobs = self.topo_sort()
        for job in sort_jobs:
            job.set_sched( self._sched_type )
            job.submit( self._graph[job] )
       
        map_str = '' 
        for job in sort_jobs:    
            if print_map:
                map_str += '-------------------------------\n'
                map_str += "PBS: {}\nID: {}\n".format(\
                os.path.basename( job.get_script() ), job.get_sched_id() )
                for dep in self._graph[job]:
                    target = dep.get_target()
                    map_str += '{} {}\n'.format( dep.get_type(), target.get_sched_id())

                map_str += '-------------------------------\n'
        
        if print_map:
            return map_str

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

        
