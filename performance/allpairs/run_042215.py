from __future__ import division
import pandas
import matplotlib.pyplot as pplot
import numpy

def read_logfiles(fname):
    logfile = open(fname)

    for line in logfile:
        if line[0:10]=='Total Time':
            foundit = line

    line_list = foundit.split()
    return float(line_list[-1])

if __name__ == '__main__':

    Enmd32cores10DataFrame = list()
    Enmd32cores20DataFrame = list()
    Enmd32cores25DataFrame = list()
    Enmd32cores40DataFrame = list()

    Enmd32cores10DataFrame.append(pandas.read_pickle('results_400_32cores10win_stampede_2015-04-22T15:37:57.868702.pkl'))
    Enmd32cores10DataFrame.append(pandas.read_pickle('results_400_32cores10win_stampede_2015-04-22T16:13:58.349877.pkl'))
    Enmd32cores10DataFrame.append(pandas.read_pickle('results_400_32cores10win_stampede_2015-04-22T16:51:45.222876.pkl'))
    Enmd32cores20DataFrame.append(pandas.read_pickle('results_400_32cores20win_stampede_2015-04-22T17:03:13.045822.pkl'))
    Enmd32cores20DataFrame.append(pandas.read_pickle('results_400_32cores20win_stampede_2015-04-22T17:14:11.228862.pkl'))
    Enmd32cores20DataFrame.append(pandas.read_pickle('results_400_32cores20win_stampede_2015-04-22T17:25:08.184672.pkl'))
    Enmd32cores25DataFrame.append(pandas.read_pickle('results_400_32cores25win_stampede_2015-04-22T17:33:24.113940.pkl'))
    Enmd32cores25DataFrame.append(pandas.read_pickle('results_400_32cores25win_stampede_2015-04-22T17:41:38.730567.pkl'))
    Enmd32cores25DataFrame.append(pandas.read_pickle('results_400_32cores25win_stampede_2015-04-22T17:49:18.960206.pkl'))
    Enmd32cores40DataFrame.append(pandas.read_pickle('results_400_32cores40win_stampede_2015-04-22T17:54:33.804617.pkl'))
    Enmd32cores40DataFrame.append(pandas.read_pickle('results_400_32cores40win_stampede_2015-04-22T17:59:33.985887.pkl'))
    Enmd32cores40DataFrame.append(pandas.read_pickle('results_400_32cores40win_stampede_2015-04-22T18:05:06.219805.pkl'))


    enmd10 = ((Enmd32cores10DataFrame[0].last_finished_abs[2] - Enmd32cores10DataFrame[0].first_started_abs[2]).total_seconds() + 
              (Enmd32cores10DataFrame[1].last_finished_abs[2] - Enmd32cores10DataFrame[1].first_started_abs[2]).total_seconds() + 
              (Enmd32cores10DataFrame[2].last_finished_abs[2] - Enmd32cores10DataFrame[2].first_started_abs[2]).total_seconds())/3
    enmd20 = ((Enmd32cores20DataFrame[0].last_finished_abs[2] - Enmd32cores20DataFrame[0].first_started_abs[2]).total_seconds() + 
              (Enmd32cores20DataFrame[1].last_finished_abs[2] - Enmd32cores20DataFrame[1].first_started_abs[2]).total_seconds() + 
              (Enmd32cores20DataFrame[2].last_finished_abs[2] - Enmd32cores20DataFrame[2].first_started_abs[2]).total_seconds())/3
    enmd25 = ((Enmd32cores25DataFrame[0].last_finished_abs[2] - Enmd32cores25DataFrame[0].first_started_abs[2]).total_seconds() + 
              (Enmd32cores25DataFrame[1].last_finished_abs[2] - Enmd32cores25DataFrame[1].first_started_abs[2]).total_seconds() + 
              (Enmd32cores25DataFrame[2].last_finished_abs[2] - Enmd32cores25DataFrame[2].first_started_abs[2]).total_seconds())/3
    enmd40 = ((Enmd32cores40DataFrame[0].last_finished_abs[2] - Enmd32cores40DataFrame[0].first_started_abs[2]).total_seconds() + 
              (Enmd32cores40DataFrame[1].last_finished_abs[2] - Enmd32cores40DataFrame[1].first_started_abs[2]).total_seconds() + 
              (Enmd32cores40DataFrame[2].last_finished_abs[2] - Enmd32cores40DataFrame[2].first_started_abs[2]).total_seconds())/3

    print enmd10
    print enmd20
    print enmd25
    print enmd40

    pilot10 = 0
    pilot10 = pilot10 + read_logfiles('output_400_10_traj_stampede_32cores_2015-04-22T07:32:36.771040.log')
    pilot10 = pilot10 + read_logfiles('output_400_10_traj_stampede_32cores_2015-04-22T07:50:11.214004.log')
    pilot10 = pilot10 + read_logfiles('output_400_10_traj_stampede_32cores_2015-04-22T08:09:04.508281.log')
    pilot20 = 0
    pilot20 = pilot20 + read_logfiles('output_400_20_traj_stampede_32cores_2015-04-22T07:37:11.569703.log')
    pilot20 = pilot20 + read_logfiles('output_400_20_traj_stampede_32cores_2015-04-22T07:54:55.595008.log')
    pilot20 = pilot20 + read_logfiles('output_400_20_traj_stampede_32cores_2015-04-22T08:13:35.163885.log')
    pilot25 = 0
    pilot25 = pilot25 + read_logfiles('output_400_25_traj_stampede_32cores_2015-04-22T07:41:09.978321.log')
    pilot25 = pilot25 + read_logfiles('output_400_25_traj_stampede_32cores_2015-04-22T07:58:51.617738.log')
    pilot25 = pilot25 + read_logfiles('output_400_25_traj_stampede_32cores_2015-04-22T08:17:56.542611.log')
    pilot40 = 0
    pilot40 = pilot40 + read_logfiles('output_400_40_traj_stampede_32cores_2015-04-22T07:44:54.601235.log')
    pilot40 = pilot40 + read_logfiles('output_400_40_traj_stampede_32cores_2015-04-22T08:03:06.361545.log')
    pilot40 = pilot40 + read_logfiles('output_400_40_traj_stampede_32cores_2015-04-22T08:21:52.066721.log')

    pilot10=pilot10/3
    pilot20=pilot20/3
    pilot25=pilot25/3
    pilot40=pilot40/3

    print pilot10
    print pilot20
    print pilot25
    print pilot40

    cus = [45, 120, 190, 780]
    
    n_groups=4
    index = numpy.arange(n_groups)
    bar_width = 0.35
    opacity = 1.0
    fig1 = pplot.figure()
    pplot.bar(index,[enmd40,enmd25,enmd20,enmd10],bar_width,alpha=opacity,color='b',label='EnMD')
    pplot.bar(index+bar_width,[pilot40,pilot25,pilot20,pilot10],bar_width,alpha=opacity,color='r',label='Pilot Script')
    pplot.xlabel('Window Size/Number of CUs/Pilot Size')
    pplot.ylabel('Seconds')
    pplot.xticks(index + bar_width, ('40/45/32', '25/120/32', '20/190/32', '10/780/32'))
    pplot.legend(loc=2)
    pplot.ylim(ymax=2500)
    pplot.title('EnMD vs Radical Pilot TTC for different window sizes')
    fig1.savefig('comp.png')
    