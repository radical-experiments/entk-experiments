import os, sys
import rpy2
import rpy2.robjects as robjects
import traceback

from glob import glob
from rpy2.robjects.packages import STAP
from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager, Profiler

'''
EnTK 0.6 script - Analog Ensemble application

In this example, we intend to execute 32 canalogs tasks each using 4 cores and different station IDs on Stampede
with a total resource reservation of 32 cores. Once completed, we determine the execution time of the
tasks using the EnTK profiler.
'''

def test_initial_config(d):

    possible_keys = [   'file.forecast', 'file.observation','output.AnEn',
                        'stations.ID', 'cores', 'test.ID.start', 'test.ID.end',
                        'train.ID.start', 'train.ID.end', 'rolling',
                        'members.size'
                    ]

    all_ok = True

    for keys in possible_keys:

        if keys not in d:

            print 'Expected key %s not in initial_config dictionary'%keys
            all_ok = False


    return all_ok


def process_initial_config(initial_config):

    initial_config['stations.ID'] = ' '.join([str(int(k)) for k in list(initial_config['stations.ID'])])

    possible_keys = [   'file.forecast', 'file.observation','output.AnEn',
                        'cores', 'test.ID.start', 'test.ID.end',
                        'train.ID.start', 'train.ID.end', 'rolling',
                        'members.size'
                    ]

    for keys in possible_keys:
        initial_config[keys] = initial_config[keys][0]


    for key, val in initial_config.iteritems():
        if type(val) not in [str,int, float]:
            sys.exit(1)


    return initial_config


if __name__ == '__main__':


    # Our application currently will contain only one pipeline
    p = Pipeline()


    # -------------------------- Stage 1 ---------------------------------------
    # Read initial configuration from R function
    with open('setup.R', 'r') as f:
        R_code = f.read()
    initial_config = STAP(R_code, 'initial_config')
    config = initial_config.initial_config(False)
    initial_config = dict(zip(config.names, list(config)))


    if not test_initial_config(initial_config):
        sys.exit(1)

    initial_config = process_initial_config(initial_config)
    

    #################################################
    # additional conversion from for the dictionary #
    #################################################

    
    # First stage corresponds to the AnEn computation
    s1 = Stage()

    # List to catch all the uids of the AnEn tasks
    anen_task_uids = list()

    for ind in range(1):

        # Create a new task
        t1 = Task()
        # task executable
        t1.executable    = ['canalogs']       
        # All modules to be loaded for the executable to be detected
        t1.pre_exec      = [ 'module load gcc',      
                            'module load boost',    
                            'export PATH=/home1/04672/tg839717/git/CAnalogsV2/build:$PATH']
        # Number of cores for this task
        t1.cores         = int(initial_config['cores'])
        # List of arguments to the executable      
        t1.arguments     = [ '-N','-p',
                            '--forecast-nc', initial_config['file.forecast'],
                            '--observation-nc', initial_config['file.observation'],
                            '-o', initial_config['output.AnEn'],
                            '--stations-ID',initial_config['stations.ID'],
                            '--number-of-cores', initial_config['cores'],
                            '--test-ID-start', initial_config['test.ID.start'],
                            '--test-ID-end', initial_config['test.ID.end'],
                            '--train-ID-start', initial_config['train.ID.start'],
                            '--train-ID-end', initial_config['train.ID.end'],
                            '--rolling', initial_config['rolling'],
                            '--members-size',initial_config['members.size']]

        # Add this task to our stage
        s1.add_tasks(t1)

    # Add the stage to our pipeline
    p.add_stages(s1)
    # --------------------------------------------------------------------------


    # -------------------------- Stage 2 ---------------------------------------
    

    # Third stage corresponds to evaluation of interpolated data
    s2 = Stage()

    t2 = Task()
    t2.executable    = ['python']
    t2.pre_exec      =  ['module load R',
                        'module load Rstudio',
                        'module load Rstats',
                        'module load pyton',
                        'source $HOME/ve_rpy/bin/activate']
    t2.cores         = 1
    t2.arguments     = [ 'evaluation.py', 
                        '--file_observation', initial_config['file.observation'],
                        '--file_AnEn', initial_config['file.observation'],
                        '--stations_ID', initial_config['stations.ID'],
                        '--test_ID_start', initial_config['test.ID.start'],
                        '--test_ID_end', initial_config['test.ID.end'],
                        '--nflts', '8',
                        '--nrows', '100',
                        '--ncols', '100'
                    ]
    t2.upload_input_data = ['./evaluation.py', './evaluation.R']
    t2.link_input_data = ['$PIPELINE_%s_STAGE_%s_TASK_%s/%s'%(p.uid, s1.uid, t1.uid, initial_config['output.AnEn'])]

    s2.add_tasks(t2)

    p.add_stages(s2)
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
