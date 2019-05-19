import argparse
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#### ---- PCB Creation ---- ####
class PCB():
    def __init__(self,arrival_time,proc,pid,state,priority,interruptable,est_tot_time,est_remain_time):
        self.arrival_time = arrival_time
        self.proc = proc
        self.pid = pid
        self.state = state
        self.priority = priority
        self.interruptable = interruptable
        self.est_tot_time = est_tot_time
        self.est_remain_time = est_remain_time

        #record keeping
        self.time_in_cpu = 0
        self.time_completed = None
        self.cpu_return_time = None
    
    def __repr__(self):
        attr = [self.arrival_time,self.proc,self.pid,self.state,self.priority,self.interruptable,self.est_tot_time,self.est_remain_time,self.time_in_cpu]
        return ','.join(list(map(str,attr)))

def proc_to_pcb(process_strings):
    '''list of process strings -> list of PCB objects '''
    pcbs = []
    for line in process_strings:
        proc = line.replace('\n','').split(',')
        pcb = PCB(int(proc[0]),proc[1],int(proc[2]),proc[3],int(proc[4]),int(proc[5]),int(proc[6]),int(proc[7]))
        pcbs.append(pcb)
    return(pcbs)
#### ---- PCB Creation ---- ####

#### ---- Statistics ---- ####
def record_proc(proc):
    '''return string: proc_name,time_completed,runtime,turn_around,wait_time'''
    time_completed = str(proc.time_completed)
    runtime = str(proc.time_in_cpu)
    turn_around = int(proc.time_completed) - int(proc.arrival_time)
    wait_time = turn_around - int(proc.time_in_cpu)
    return ','.join([proc.proc, time_completed, runtime, str(turn_around), str(wait_time)]) + '\n'

def record_to_file(alg,record_list,ctx_count,rr=None):
    '''Write list of record_proc outputs to file and return filename'''
    if rr:
        filename = alg.__name__ + '_' + str(rr) + '.csv'
    else:
        filename = alg.__name__ + '.csv'

    with open(filename,'w') as file:
        file.write('context_switches:'+ str(ctx_count)+'\n')
        file.write("proc,completion_time,runtime,turnaround,wait\n")
        for entry in record_list:
            file.write(entry)
    return filename

def file_stats(filename):
    '''Return dictionary of stastistics based on output of record_to_file'''
    stats = {}

    with open(filename) as f:
        ctx_count = int(f.readline().split(':')[1].replace('\n',''))
    df = pd.read_csv(filename,skiprows=1)

    stats['turnaround_avg'] = int(df.turnaround.mean())
    stats['wait_avg'] = int(df.wait.mean())
    stats['wait_max'] = df.wait.max()
    stats['total_runtime'] = df.runtime.sum() + 2 * ctx_count
    stats['ctx_count'] = ctx_count

    return stats

#### ---- Statistics ---- ####

##### ---- Algortithms ---- #####
def fcfs(pcb_list):
    '''First come first serve'''
    chosen_pcb = sorted(pcb_list, key=lambda pcb: pcb.arrival_time)[0]
    return pcb_list.pop(pcb_list.index(chosen_pcb))

def sjn(pcb_list):
    '''Shortest job next'''
    #NOTE: Functions as SRTN when time qunatum used
    chosen_pcb = sorted(pcb_list, key=lambda pcb: pcb.est_remain_time)[0]
    return pcb_list.pop(pcb_list.index(chosen_pcb))

def priority(pcb_list):
    '''Highest Priority'''
    chosen_pcb = sorted(pcb_list, key=lambda pcb: pcb.priority)[0]
    return pcb_list.pop(pcb_list.index(chosen_pcb))

##### ---- Algortithms ---- #####

#### ---- Scheduling / CPU ---- ####
def add_incoming(ready_list,incoming_list,cpu_return_list,current_time):
    '''Add processes to ready_list that arrived while CPU was busy'''

    arrived_list = []

    #move from incoming_list -> arrived_list
    if len(incoming_list) > 0:
        for proc in incoming_list:
            if proc.arrival_time <= current_time:
               arrived_list.append(incoming_list.pop(incoming_list.index(proc)))
    
    #move from cpu_return_list -> arrived list
    if len(cpu_return_list) > 0:
        for proc in cpu_return_list:
            if proc.cpu_return_time <= current_time:
                arrived_list.append(cpu_return_list.pop(cpu_return_list.index(proc)))

    def sort_criteria(pcb):
        if pcb.cpu_return_time != None:
            return pcb.cpu_return_time
        else:
            return pcb.arrival_time

    #Order PCBs correctly for ready_list
    arrived_list.sort(key=sort_criteria)

    #append to ready_list
    for proc in arrived_list:
        ready_list.append(proc)

def scheduler(alg,tq=None):

    #Populate Initial Queue
    with open('processes_3.txt') as file:
         process_list = file.readlines()[1:]

    #admin
    current_time = 0
    incoming_list = proc_to_pcb(process_list)
    ready_list = []
    cpu_return_list = []

    #stats
    ctx_count = 0
    record_list = []

    while ready_list or incoming_list:
        #add processes to ready list as they arrive, or return from cpu
        add_incoming(ready_list,incoming_list,cpu_return_list,current_time)

        if ready_list:
            
            #choose pcb from ready_list based on alg
            chosen_proc = alg(ready_list)

            #RR
            if tq: 
                current_time += cpu(chosen_proc,current_time,until_completion=False,quantum=tq)
            
            #Until Completion
            else:
                current_time += cpu(chosen_proc,current_time)

            #add proc back into arrival queue if RR
            if chosen_proc.est_remain_time > 0:
                cpu_return_list.append(chosen_proc)

                #context switch
                current_time += 2 
                ctx_count += 1

            #record for stats keeping
            else: 
                record_list.append(record_proc(chosen_proc))

        current_time += 1
    return record_to_file(alg,record_list,ctx_count,tq)

def cpu(pcb, current_time, until_completion=True, quantum = None,):
    '''Decrement PCB est_remain_time and perform some necesary admin'''

    #non preemptive
    if until_completion:
        time_in_cpu = pcb.est_remain_time
        pcb.est_remain_time = 0

    #RR
    else:
        assert(quantum != None)
        new_remain_time = pcb.est_remain_time - quantum

        if new_remain_time < 0:
            time_in_cpu = quantum + new_remain_time
            pcb.est_remain_time = 0
             
        else:
            time_in_cpu = quantum
            pcb.est_remain_time = new_remain_time

    #Stats
    if pcb.est_remain_time == 0:
        pcb.time_completed = current_time + time_in_cpu  

    pcb.cpu_return_time = current_time + time_in_cpu
    pcb.time_in_cpu += time_in_cpu

    return(time_in_cpu)
#### ---- Scheduling / CPU ---- #### 

#### ---- Main ---- ####
def demo_plot(results):
    labels = []
    turnaround_avg = []
    wait_avg = []
    wait_max = []
    ctx_count = []

    for key,val in results.items():
        labels.append(key)
        turnaround_avg.append(val['turnaround_avg'])
        wait_avg.append(val['wait_avg'])
        wait_max.append(val['wait_max'])
        ctx_count.append(val['ctx_count'])

    fig, axs = plt.subplots(2,2)
    x = np.arange(len(labels))
    colors = ['b','g','r','c','m','y','k','xkcd:royal purple']

    axs[0,0].bar(x,turnaround_avg,tick_label=labels,color=colors)
    axs[1,0].bar(x,wait_avg,tick_label=labels,color=colors)
    axs[0,1].bar(x,wait_max,tick_label=labels,color=colors)
    axs[1,1].bar(x,ctx_count,tick_label=labels,color=colors)

    ax_list = [axs[0,0],axs[1,0],axs[0,1],axs[1,1]]

    titles = ['Turaround Average','Wait Average','Wait Maximum','Context Switches']
    for ax,title in zip(ax_list,titles):
        ax.tick_params(axis='x',labelrotation=45,labelsize='x-small')

        if ax != axs[1,1]:
            ax.set_ylim(bottom=1750)
        ax.set_title(title,fontsize='small')


    fig.suptitle('CPU Scheduling Algorithms by the Numbers')
    fig.set_size_inches(12.8,9.6)
    plt.show()
    fig.savefig('cpu_scheduling_plot.png',bbox_inches='tight')

def demo_table(results):
    df = pd.DataFrame.from_dict(results,orient='index')
    print(df)

def demo():
    results = {}

    #FCFS
    results['fcfs'] = file_stats(scheduler(fcfs))
    #SJN
    results['sjn'] = file_stats(scheduler(sjn))
    #PRIORITY
    results['priority'] = file_stats(scheduler(priority))
    #FCFS RR 10 100 1000
    results['fcfs_10'] = file_stats(scheduler(fcfs,10))
    results['fcfs_100'] = file_stats(scheduler(fcfs,100))
    results['fcfs_1000'] = file_stats(scheduler(fcfs,1000))
    #SRTN 50
    results['srtn_50'] = file_stats(scheduler(sjn,50))
    #Priority RR 50
    results['priority_50'] = file_stats(scheduler(priority,50))

    demo_table(results)
    demo_plot(results)
    

def arg_parser():
    '''Returns ArgumentParser object for command line arguments'''
    parser = argparse.ArgumentParser()
    parser.add_argument("-fcfs",help="First Come First Serve Scheduling time quantum. TQ <= 0 -> non-preemptive",nargs=1)
    parser.add_argument("-sjn",help="Shortest Job Next Scheduling time quantum. TQ <= 0 -> non-preemptive",nargs=1)
    parser.add_argument("-priority",help="Priority Scheduling time quantum. TQ <= 0 -> non-preemptive",nargs=1)
    parser.add_argument("-g","--graph",help="Produce a plot", action="store_true")
    return(parser)

def main():
    parser = arg_parser()
    args = parser.parse_args()

    if args.fcfs:
        try:
            fcfs_tq = int(args.fcfs[0])
        except ValueError:
            print("fcfs: Invalid tq Integer")

        if fcfs_tq > 0:
            print('fcfs tq_{}: '.format(fcfs_tq),file_stats(scheduler(fcfs,fcfs_tq)))
        else:
            print('fcfs: ',file_stats(scheduler(fcfs)))

    if args.sjn:
        try:
            sjn_tq = int(args.sjn[0])
        except ValueError:
            print('sjn: Invalid tq Integer')

        if sjn_tq > 0:
            print('sjn tq_{}: '.format(sjn_tq),file_stats(scheduler(sjn,sjn_tq)))
        else:
            print('sjn: ',file_stats(scheduler(sjn)))
    
    if args.priority:
        try:
            priority_tq = int(args.priority[0])
        except ValueError:
            print('priortity: Invalid tq Integer')


        if priority_tq > 0:
            print('priority tq_{}: '.format(priority_tq),file_stats(scheduler(priority,priority_tq)))
        else:
            print('priority: ',file_stats(scheduler(priority)))
    
    #No args = Demo
    if not (args.fcfs and args.sjn and args.priority):
        demo()

if __name__ == '__main__':
    main()
#### ---- Main ---- ####