from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
import os
import sys
import math
# ------------------------------------------------------------------------------
# Set default verbosity


app_coll = {
    
    "grompp": {
                'executable': ['gmx grompp'],
                'arguments': ['-f','grompp.mdp','-c','input.gro','-p','topol.top','-o','topol.tpr'],
                'cores': 1
            },
    "mdrun": {
                'executable': ['gmx mdrun'],
                'arguments': ['-s','topol.tpr','-c','md.out'],
                'cores': 1
            }
}


res_coll = {

    "xsede.supermic": {
                'walltime': 60,
                'schema': 'gsissh',
                'project': 'TG-MCB090174',
                'cores_per_node': 20
                },
    }

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'


def get_pipeline(tasks):

    # Create a Pipeline object
    p = Pipeline()

    # Create a Stage 1
    s1 = Stage()

    # Create a Task object according to the app_name
    t1 = Task()
    t1.pre_exec = ['module load gromacs/5.0/INTEL-140-MVAPICH2-2.0']
    t1.executable = app_coll['grompp']['executable']
    t1.arguments = app_coll['grompp']['arguments']
    t1.cores = app_coll['grompp']['cores']
    t1.link_input_data = [
                            '$SHARED/grompp.mdp > grompp.mdp',
                            '$SHARED/input.gro > input.gro',
                            '$SHARED/topol.top > topol.top'
                        ]

    # Add the Task to the Stage
    s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)


    # Create a Stage 2
    s2 = Stage()

    for cnt in range(tasks):

        # Create a Task object according to the app_name
        t2 = Task()
        t2.pre_exec = ['module load gromacs/5.0/INTEL-140-MVAPICH2-2.0','export OMP_NUM_THREADS=%s'%num_cores]
        t2.executable = app_coll['mdrun']['executable']
        t2.arguments = app_coll['mdrun']['arguments']
        #t2.cores = app_coll['mdrun']['cores']
        t2.cores = num_cores
        t2.copy_input_data = ['$Pipeline_%s_Stage_%s_Task_%s/topol.tpr'%(p.uid, s1.uid,t1.uid)]

        # Add the Task to the Stage
        s2.add_tasks(t2)

    # Add Stage to the Pipeline
    p.add_stages(s2)

    return p

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print 'Missing arguments. Execution cmd: python poe.py <num_cores>'
        sys.exit(1)

    num_tasks = 16
    res_name = 'xsede.supermic'
    num_cores = int(sys.argv[1])

    pipes_set = set()

    pipes_set.add(get_pipeline(num_tasks))

    #pilot_cores = int((math.ceil(float(num_tasks*app_coll['mdrun']['cores'])/res_coll[res_name]['cores_per_node'])*res_coll[res_name]['cores_per_node'])+res_coll[res_name]['cores_per_node'])
    pilot_cores = int((math.ceil(float(16*num_cores)/res_coll[res_name]['cores_per_node'])*res_coll[res_name]['cores_per_node'])+res_coll[res_name]['cores_per_node'])


    print pilot_cores

    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cores and project
    # resource is 'local.localhost' to execute locally
    res_dict = {

            'resource': res_name,
            'walltime': res_coll[res_name]['walltime'],
            'cores': pilot_cores,
            'project': res_coll[res_name]['project'],
            'access_schema': res_coll[res_name]['schema']
    }

    # Create Resource Manager object with the above resource description
    rman = ResourceManager(res_dict)
    rman.shared_data = ['./ip_data/input.gro','./ip_data/grompp.mdp','./ip_data/topol.top']

    # Create Application Manager
    appman = AppManager()

    # Assign resource manager to the Application Manager
    appman.resource_manager = rman

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.assign_workflow(pipes_set)

    # Run the Application Manager
    appman.run()
