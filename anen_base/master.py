from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager, Profiler
import os
from glob import glob
import traceback

'''
EnTK 0.6 script - Analog Ensemble application

In this example, we intend to execute 32 canalogs tasks each using 4 cores and different station IDs on Stampede
with a total resource reservation of 32 cores. Once completed, we determine the execution time of the
tasks using the EnTK profiler.
'''


if __name__ == '__main__':


    # Our application currently will contain only one pipeline
    p = Pipeline()


    # -------------------------- Stage 1 ---------------------------------------
    # First stage corresponds to the AnEn computation
    s = Stage()

    # List to catch all the uids of the AnEn tasks
    anen_task_uids = list()

    for ind in range(4):

        # Create a new task
        t = Task()
        # task executable
        t.executable    = ['canalogs']       
        # All modules to be loaded for the executable to be detected
        t.pre_exec      = [ 'module load gcc',      
                            'module load boost',    
                            'export PATH=/home1/04672/tg839717/git/CAnalogsV2/build:$PATH']
        # Number of cores for this task
        t.cores         = 16
        # List of arguments to the executable      
        t.arguments     = [ '-L','-l',
                            '-d', '<unknown>',
                            '-o', '<unknown>',
                            '--stations-ID','<unknown>',
                            '--number-of-cores', '16',
                            '--test-ID-start', '<unknown>',
                            '--test-ID-end', '<unknown>',
                            '--train-ID-start', '<unknown>',
                            '--train-ID-end', '<unknown>']

        # Add this task to our stage
        s.add_tasks(t)

        # Add the processed task id to our list. 
        # The format of the task is "radical.entk.task.0000" and we only
        # need "task.0000"
        task_uids.append('.'.join(t.uid.split('.')[2:]))

    # Add the stage to our pipeline
    p.add_stages(s)
    # --------------------------------------------------------------------------


    # -------------------------- Stage 2 ---------------------------------------
    # Second stage corresponds to interpolation of data to the entire domain
    s = Stage()

    t = Task()
    t.executable    = []
    t.pre_exec      =  []
    t.cores         = 1
    t.arguments     = []

    s.add_tasks(t)

    p.add_stages(s)
    # --------------------------------------------------------------------------


    # -------------------------- Stage 3 ---------------------------------------
    # Third stage corresponds to evaluation of interpolated data
    s = Stage()

    t = Task()
    t.executable    = []
    t.pre_exec      =  []
    t.cores         = 1
    t.arguments     = []

    s.add_tasks(t)

    p.add_stages(s)
    # --------------------------------------------------------------------------


    # Create a dictionary to describe our resource request
    res_dict = {

            'resource': 'xsede.stampede',
            'walltime': 60,
            'cores': 64,
            'project': 'TG-MCB090174',
            'queue': 'development',
            'schema': 'gsissh'

    }

    try:

        # Create a Resource Manager using the above description
        rman = ResourceManager(res_dict)

        # Create an Application Manager for our application
        appman = AppManager()

        # Assign the resource manager to be used by the application manager
        appman.resource_manager = rman

        # Assign the workflow to be executed by the application manager
        appman.assign_workflow(set([p]))

        # Run the application manager -- blocking call
        appman.run()


        # Once completed, use EnTK profiler to get the time between 'SCHEDULING' and 'EXECUTING' states for all
        # tasks. This is the execution time of the tasks as seen by EnTK.
        #p = Profiler()
        #print 'Task uids: ', task_uids
        #print 'Total execution time for all tasks: ', p.duration(objects = task_uids, states=['SCHEDULING', 'EXECUTED'])

    except Exception, ex:

        print 'Execution failed, error: %s'%ex
        print traceback.format_exc()

    finally:

        profs = glob('./*.prof')
        for f in profs:
            os.remove(f)