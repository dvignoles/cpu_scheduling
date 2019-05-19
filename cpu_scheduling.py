import argparse
import pandas as pd

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
    '''return string proc,turn_around,wait_time'''
    time_completed = str(proc.time_completed)
    runtime = str(proc.time_in_cpu)
    turn_around = int(proc.time_completed) - int(proc.arrival_time)
    wait_time = turn_around - int(proc.time_in_cpu)
    return ','.join([proc.proc, time_completed, runtime, str(turn_around), str(wait_time)]) + '\n'

def record_to_file(alg,record_list,ctx_count,rr=None):
    '''Write record_list to file and return file statistics'''
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

    if until_completion:
        time_in_cpu = pcb.est_remain_time
        pcb.est_remain_time = 0
    else:
        assert(quantum != None)
        new_remain_time = pcb.est_remain_time - quantum

        if new_remain_time < 0:
            time_in_cpu = quantum + new_remain_time
            pcb.est_remain_time = 0
             
        else:
            time_in_cpu = quantum
            pcb.est_remain_time = new_remain_time

    if pcb.est_remain_time == 0:
        pcb.time_completed = current_time + time_in_cpu  

    pcb.cpu_return_time = current_time + time_in_cpu
    pcb.time_in_cpu += time_in_cpu #stats keeping

    return(time_in_cpu)
#### ---- Scheduling / CPU ---- #### 

#### ---- Main ---- ####
def arg_parser():
    '''Returns ArgumentParser object for command line arguments'''
    parser = argparse.ArgumentParser()
    parser.add_argument("-fcfs",help="First Come First Serve Scheduling",nargs=1)
    parser.add_argument("-sjn",help="Shortest Job Next Scheduling",nargs=1)
    parser.add_argument("-priority",help="Priority Scheduling",nargs=1)
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

if __name__ == '__main__':
    main()
#### ---- Main ---- ####