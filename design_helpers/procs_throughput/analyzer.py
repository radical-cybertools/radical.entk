import glob

num_push_procs = 3
num_pop_procs = 1
num_queues = 1

DATA = './push_%s_pop_%s_q_%s'%(num_push_procs, num_pop_procs, num_queues)

if __name__ == '__main__':

    pushers = glob.glob('%s/push_*.txt'%DATA)
    for fname in pushers:

        f = open(fname,'r')
        times = f.readlines()

        start_time = float(times[0])
        #print times[0]
        cnt=1

        name = fname.split('/')[2].strip().split('.')[0].strip()
        for val in times[1:]:

            cur_time = float(val)
            cnt+=1

            if cnt%100000 == 0:
                print '%s: Throughput: %s'%(name, float(cnt/(cur_time-start_time)))


    poppers = glob.glob('%s/pop_*.txt'%DATA)
    for fname in poppers:

        f = open(fname,'r')
        times = f.readlines()

        start_time = float(times[0])
        cnt=1

        name = fname.split('/')[2].strip().split('.')[0].strip()
        #print name
        for val in times[1:]:

            cur_time = float(val)
            cnt+=1

            if cnt%100000 == 0:
                print '%s: Throughput: %s'%(name, float(cnt/(cur_time-start_time)))