from radical.entk import Task
import sys
import time
from multiprocessing import Process
import multiprocessing as mp
import os
import shutil
from Queue import Empty
from pympler import asizeof
import pika
import traceback
import uuid
import json
import psutil

kill_pusher = mp.Event()
kill_popper = mp.Event()

MAX_TASKS=1048576

#MAX_TASKS=1024

def push_function(ind, num_push, num_queues):

    try:

        mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=32769))
        mq_channel = mq_connection.channel()
       
        tasks_pushed = 0
        global MAX_TASKS

        proc_tasks = MAX_TASKS/num_push

        push_times = []
        proc_mem = []
        t = Task()
        t.arguments = ["--template=PLCpep7_template.mdp",
                        "--newname=PLCpep7_run.mdp",
                        "--wldelta=100",
                        "--equilibrated=False",
                        "--lambda_state=0",
                        "--seed=1"]

        t.cores = 20
        t.copy_input_data = ['$STAGE_2_TASK_1/PLCpep7.tpr']
        t.download_output_data = ['PLCpep7.xtc > PLCpep7_run1_gen0.xtc',
                                    'PLCpep7.log > PLCpep7_run1_gen0.log',
                                    'PLCpep7_dhdl.xvg > PLCpep7_run1_gen0_dhdl.xvg',
                                    'PLCpep7_pullf.xvg > PLCpep7_run1_gen0_pullf.xvg',
                                    'PLCpep7_pullx.xvg > PLCpep7_run1_gen0_pullx.xvg',
                                    'PLCpep7.gro > PLCpep7_run1_gen0.gro'
                                ]


        t_dict = t.to_dict()

        print 'Size of task: ', asizeof.asizeof(t_dict)


        name = 'queue_%s'%(ind%num_queues)

        while (tasks_pushed < proc_tasks)and(not kill_pusher.is_set()):            

            corr_id = str(uuid.uuid4())

            obj = { 'task': t_dict, 'id': corr_id}

            mq_channel.basic_publish(   exchange='',
                                        routing_key=name,
                                        properties=pika.BasicProperties(correlation_id = corr_id),
                                        body=json.dumps(obj)
                                    )

            tasks_pushed +=1
            cur_time = time.time()

            push_times.append(cur_time)
            mem = psutil.virtual_memory().available/(2**20) # MBytes
            proc_mem.append(mem)

            #    print '%s: Push average throughput: %s tasks/sec'%(name, 
            #        float(tasks_pushed/(cur_time - start_time)))       
    
        print 'Push: ',tasks_pushed

        f = open(DATA + '/push_%s.txt'%ind,'w')
        for i in range(len(push_times)):
            f.write('%s %s\n'%(push_times[i],proc_mem[i]))
            #f.write('%s\n'%(push_times[ind]))
        f.close()

        print 'Push proc killed'



    except KeyboardInterrupt:

        print len(push_times)

        f = open(DATA + '/push_%s.txt'%ind,'w')
        for i in range(min(len(push_times),len(proc_mem))):
            f.write('%s %s\n'%(push_times[i], proc_mem[i]))
        f.close()

        print 'Push proc killed'

    except Exception as ex:

        print 'Unexpected error: %s'%ex
        print traceback.format_exc()

        f = open(DATA + '/push_%s.txt'%ind,'w')
        for i in range(min(len(push_times), len(proc_mem))):
            f.write('%s %s\n'%(push_times[i], proc_mem[i]))
        f.close()

        



def pop_function(ind, num_pop, num_queues):

    try:

        start_time = time.time()

        tasks_popped=0
        global MAX_TASKS

        proc_tasks = MAX_TASKS/num_pop

        pop_times = []
        proc_mem = []

        mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=32769))
        mq_channel = mq_connection.channel()

        name = 'queue_%s'%(ind%num_queues)

        while (tasks_popped < proc_tasks)and(not kill_popper.is_set()):

            method_frame, props, body = mq_channel.basic_get(queue=name)       

            if body:

                obj = json.loads(body)

                if obj['id'] == props.correlation_id:

                    mq_channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                    tasks_popped +=1
                    cur_time = time.time()

                    pop_times.append(cur_time)                
                    mem = psutil.virtual_memory().available/(2**20) # MBytes
                    proc_mem.append(mem)
            

        print 'Popper: ', tasks_popped

        f = open(DATA + '/pop_%s.txt'%ind,'w')

        for i in range(len(pop_times)):
            f.write('%s %s\n'%(pop_times[i], proc_mem[i]))
        f.close()

        print 'Pop proc killed'


    except KeyboardInterrupt:

        print len(pop_times)

        f = open(DATA + '/pop_%s.txt'%name,'w')
        for i in range(min(len(pop_times), len(proc_mem))):
            f.write('%s %s\n'%(pop_times[i], proc_mem[i]))
        f.close()

        print 'Pop proc killed'

    except Exception as ex:

        print 'Unexpected error: %s'%ex
        print traceback.format_exc()

        f = open(DATA + '/pop_%s.txt'%name,'w')
        for i in range(min(len(pop_times), len(proc_mem))):
            f.write('%s %s\n'%(pop_times[i], proc_mem[i]))
        f.close()

        print 'Unexpected error: %s'%ex


if __name__ == '__main__':

    
    if len(sys.argv) != 4:
        print 'Usage: python runme.py <push procs> <pull procs> <num_queues>'
        sys.exit(1)

    num_push_procs = int(sys.argv[1])
    num_pop_procs = int(sys.argv[2])
    num_queues = int(sys.argv[3])

    if num_queues > num_push_procs or num_queues > num_pop_procs:
        print 'Too many queues'
        sys.exit(1)


    trials=3

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=32769))
    mq_channel = mq_connection.channel()

    try:

        for i in range(2, trials+1):

            DATA = './push_%s_pop_%s_q_%s_trial_%s'%(num_push_procs, num_pop_procs, num_queues,i)

            try:
                shutil.rmtree(DATA)
            except:
                pass

            os.makedirs(DATA)
        
            push_procs = list()
            pop_procs = list()


            # Start popping procs and assign queues
            for t in range(num_queues):

                cur_q = t   # index of queue to be used
                name = 'queue_%s'%(t)

                mq_channel.queue_delete(queue=name)
                mq_channel.queue_declare(queue=name)

                
            for t in range(num_pop_procs):

                name = 'pop_%s'%t
                #t1 = procing.Thread(target=pop_function, args=(q_list[cur_q],name), name=name)
                t1 = Process(target=pop_function, args=(t,num_pop_procs, num_queues), name=name)
                t1.start()
                pop_procs.append(t1)

            print 'Pop procs created'

            for t in range(num_push_procs):

                name = 'push_%s'%t
                t2 = Process(target=push_function, args=(t, num_push_procs, num_queues), name=name)
                t2.start()
                push_procs.append(t2)

        
            print 'Push procs created'

            for t in push_procs:
                t.join()

            for t in pop_procs:
                t.join()

    except KeyboardInterrupt:
        print 'Main process killed'
        kill_pusher.set()
        kill_popper.set()
        for t in pop_procs:
            t.join()

        for t in push_procs:
            t.join()

    except Exception as ex:
        
        print 'Unexpected error: %s'%ex
        print traceback.format_exc()

        kill_pusher.set()
        kill_popper.set()
        for t in pop_procs:
            t.join()

        for t in push_procs:
            t.join()
