from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
import os
import sys
# ------------------------------------------------------------------------------
# Set default verbosity


app_dict = {
    
    "sleep": {
                'executable': '/bin/sleep',
                'arguments': 10,
                'cores': 1
            }
}

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'

if __name__ == '__main__':


    if len(sys.argv) != 2:
        print 'Missing arguments. Execution cmd: python poe.py <num_tasks> <app_name>'
        sys.exit(1)

    num_tasks = int(sys.argv[2])
    app_name = sys.argv[3]

    num_tas

    # Create a Pipeline object
    p = Pipeline()

    # Create a Stage 1
    s1 = Stage()

    for cnt in range(num_tasks):

        # Create a Task object according to the app_name
        t1 = Task()    
        t1.executable = [app_dict[app_name]['executable']] 
        t1.arguments = [app_dict[app_name]['arguments']]
        t1.cores = [app_dict[app_name]['cores']]

        # Add the Task to the Stage
        s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)


    # Create another Stage object to hold character count tasks
    s2 = Stage()

    for cnt in range(num_tasks):

        # Create a Task object according to the app_name
        t1 = Task()    
        t1.executable = [app_dict[app_name]['executable']] 
        t1.arguments = [app_dict[app_name]['arguments']]
        t1.cores = [app_dict[app_name]['cores']]

        # Add the Task to the Stage
        s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s2)

    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cores and project
    # resource is 'local.localhost' to execute locally
    res_dict = {

            'resource': 'local.localhost',
            'walltime': 10,
            'cores': 2,
            'project': '',
    }

    # Create Resource Manager object with the above resource description
    rman = ResourceManager(res_dict)

    # Create Application Manager
    appman = AppManager()

    # Assign resource manager to the Application Manager
    appman.resource_manager = rman

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.assign_workflow(set([p]))

    # Run the Application Manager
    appman.run()
