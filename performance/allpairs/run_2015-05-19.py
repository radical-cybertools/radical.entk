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

    Enmd16cores10DataFrame = list()
    Enmd16cores20DataFrame = list()
    Enmd16cores25DataFrame = list()
    Enmd16cores40DataFrame = list()

    Enmd16cores10DataFrame.append(pandas.read_pickle('results_400_16cores10win_stampede_2015-05-20T01:33:39.576254.pkl'))
    Enmd16cores10DataFrame.append(pandas.read_pickle('results_400_16cores10win_stampede_2015-05-20T02:03:31.970909.pkl'))
    Enmd16cores10DataFrame.append(pandas.read_pickle('results_400_16cores10win_stampede_2015-05-20T02:32:33.518513.pkl'))
    Enmd16cores20DataFrame.append(pandas.read_pickle('results_400_16cores20win_stampede_2015-05-20T00:28:56.623049.pkl'))
    Enmd16cores20DataFrame.append(pandas.read_pickle('results_400_16cores20win_stampede_2015-05-20T00:40:21.922339.pkl'))
    Enmd16cores20DataFrame.append(pandas.read_pickle('results_400_16cores20win_stampede_2015-05-20T00:57:47.440979.pkl'))
    Enmd16cores25DataFrame.append(pandas.read_pickle('results_400_16cores25win_stampede_2015-05-20T00:04:36.742870.pkl'))
    Enmd16cores25DataFrame.append(pandas.read_pickle('results_400_16cores25win_stampede_2015-05-20T00:10:45.027508.pkl'))
    Enmd16cores25DataFrame.append(pandas.read_pickle('results_400_16cores25win_stampede_2015-05-20T00:19:10.601136.pkl'))
    Enmd16cores40DataFrame.append(pandas.read_pickle('results_400_16cores40win_stampede_2015-05-19T23:45:37.201291.pkl'))
    Enmd16cores40DataFrame.append(pandas.read_pickle('results_400_16cores40win_stampede_2015-05-19T23:51:28.954710.pkl'))
    Enmd16cores40DataFrame.append(pandas.read_pickle('results_400_16cores40win_stampede_2015-05-19T23:57:13.158889.pkl'))


    enmd10 = ((Enmd16cores10DataFrame[0].last_finished_abs[2] - Enmd16cores10DataFrame[0].first_started_abs[2]).total_seconds() + 
              (Enmd16cores10DataFrame[1].last_finished_abs[2] - Enmd16cores10DataFrame[1].first_started_abs[2]).total_seconds() + 
              (Enmd16cores10DataFrame[2].last_finished_abs[2] - Enmd16cores10DataFrame[2].first_started_abs[2]).total_seconds())/3
    enmd20 = ((Enmd16cores20DataFrame[0].last_finished_abs[2] - Enmd16cores20DataFrame[0].first_started_abs[2]).total_seconds() + 
              (Enmd16cores20DataFrame[1].last_finished_abs[2] - Enmd16cores20DataFrame[1].first_started_abs[2]).total_seconds() + 
              (Enmd16cores20DataFrame[2].last_finished_abs[2] - Enmd16cores20DataFrame[2].first_started_abs[2]).total_seconds())/3
    enmd25 = ((Enmd16cores25DataFrame[0].last_finished_abs[2] - Enmd16cores25DataFrame[0].first_started_abs[2]).total_seconds() + 
              (Enmd16cores25DataFrame[1].last_finished_abs[2] - Enmd16cores25DataFrame[1].first_started_abs[2]).total_seconds() + 
              (Enmd16cores25DataFrame[2].last_finished_abs[2] - Enmd16cores25DataFrame[2].first_started_abs[2]).total_seconds())/3
    enmd40 = ((Enmd16cores40DataFrame[0].last_finished_abs[2] - Enmd16cores40DataFrame[0].first_started_abs[2]).total_seconds() + 
              (Enmd16cores40DataFrame[1].last_finished_abs[2] - Enmd16cores40DataFrame[1].first_started_abs[2]).total_seconds() + 
              (Enmd16cores40DataFrame[2].last_finished_abs[2] - Enmd16cores40DataFrame[2].first_started_abs[2]).total_seconds())/3

    print enmd10
    print enmd20
    print enmd25
    print enmd40

    pilot10 = 0
    pilot10 = pilot10 + read_logfiles('output_400_10_traj_stampede_16cores_2015-05-20T05:11:28.515153.log')
    pilot10 = pilot10 + read_logfiles('output_400_10_traj_stampede_16cores_2015-05-20T05:36:41.439699.log')
    pilot10 = pilot10 + read_logfiles('output_400_10_traj_stampede_16cores_2015-05-20T05:59:17.958928.log')
    pilot20 = 0
    pilot20 = pilot20 + read_logfiles('output_400_20_traj_stampede_16cores_2015-05-20T04:28:38.501875.log')
    pilot20 = pilot20 + read_logfiles('output_400_20_traj_stampede_16cores_2015-05-20T04:34:59.023918.log')
    pilot20 = pilot20 + read_logfiles('output_400_20_traj_stampede_16cores_2015-05-20T04:41:05.286570.log')
    pilot25 = 0
    pilot25 = pilot25 + read_logfiles('output_400_25_traj_stampede_16cores_2015-05-20T04:09:48.523801.log')
    pilot25 = pilot25 + read_logfiles('output_400_25_traj_stampede_16cores_2015-05-20T04:15:51.023123.log')
    pilot25 = pilot25 + read_logfiles('output_400_25_traj_stampede_16cores_2015-05-20T04:22:00.405062.log')
    pilot40 = 0
    pilot40 = pilot40 + read_logfiles('output_400_40_traj_stampede_16cores_2015-05-20T02:53:41.534756.log')
    pilot40 = pilot40 + read_logfiles('output_400_40_traj_stampede_16cores_2015-05-20T03:19:43.909100.log')
    pilot40 = pilot40 + read_logfiles('output_400_40_traj_stampede_16cores_2015-05-20T03:34:19.350119.log')

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
    pplot.xticks(index + bar_width, ('40/45/16', '25/120/16', '20/190/16', '10/780/16'))
    pplot.legend(loc=2)
    pplot.ylim(ymax=2500)
    pplot.title('EnMD vs Radical Pilot TTC for different window sizes')
    fig1.savefig('comp.png')
    