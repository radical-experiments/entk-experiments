from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
import os
import sys
# ------------------------------------------------------------------------------
# Set default verbosity


app_coll = {
    
    "sleep": {
                'executable': '/bin/sleep',
                'arguments': 1,
                'cores': 1
            }
}


res_coll = {

    "xsede.supermic": {
                'walltime': 60,
                'schema': 'gsissh',
                'project': 'TG-MCB090174'
                },

    "local.localhost": {
                'walltime': 60,
                'schema': None,
                'project': None
            }
}

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'


def get_pipeline(tasks):

    # Create a Pipeline object
    p = Pipeline()

    # Create a Stage 1
    s1 = Stage()

    for cnt in range(tasks):

        # Create a Task object according to the app_name
        t1 = Task()
        t1.executable = [app_coll[app_name]['executable']]
        t1.arguments = [app_coll[app_name]['arguments']]
        t1.cores = app_coll[app_name]['cores']

        # Add the Task to the Stage
        s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)

    return p

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print 'Missing arguments. Execution cmd: python poe.py <num_tasks> <app_name> <resource_name>'
        sys.exit(1)

    num_tasks = int(sys.argv[1])
    app_name = sys.argv[2]
    res_name = sys.argv[3]

    pipes_set = set()

    pipes_set.add(get_pipeline(num_tasks))

    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cores and project
    # resource is 'local.localhost' to execute locally
    res_dict = {

            'resource': res_name,
            'walltime': res_coll[res_name]['walltime'],
            'cores': num_tasks,
            'project': res_coll[res_name]['project'],
            'access_schema': res_coll[res_name]['schema']
    }

    # Create Resource Manager object with the above resource description
    rman = ResourceManager(res_dict)

    # Create Application Manager
    appman = AppManager()

    # Assign resource manager to the Application Manager
    appman.resource_manager = rman

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.assign_workflow(pipes_set)

    # Run the Application Manager
    appman.run()
