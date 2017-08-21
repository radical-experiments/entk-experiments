from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
import os

# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'

if __name__ == '__main__':

    # Create a Pipeline object
    p = Pipeline()

    # Create a Stage object 
    s1 = Stage()
    s1.name = 'Stage 1'

    for cnt in range(10):

        # Create a Task object
        t = Task()
        t.name = 'my-task'        # Assign a name to the task (optional)
        t.executable = ['/bin/echo']   # Assign executable to the task   
        t.arguments = ['I am task %s in %s'%(cnt, s1.name)]  # Assign arguments for the task executable

        # Add the Task to the Stage
        s1.add_tasks(t)

    # Add Stage to the Pipeline
    p.add_stages(s1)


    # Create another Stage object
    s2 = Stage()
    s2.name = 'Stage 2'

    for cnt in range(5):

        # Create a Task object
        t = Task()
        t.name = 'my-task'        # Assign a name to the task (optional)
        t.executable = ['/bin/echo']   # Assign executable to the task   
        t.arguments = ['I am task %s in %s'%(cnt, s2.name)]  # Assign arguments for the task executable

        # Add the Task to the Stage
        s2.add_tasks(t)

    # Add Stage to the Pipeline
    p.add_stages(s2)


    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cores and project
    # resource is 'local.localhost' to execute locally
    res_dict = {

            'resource': 'local.localhost',
            'walltime': 10,
            'cores': 1,
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
