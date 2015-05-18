import sys
import time
import numpy
import os
import datetime
from textwrap import wrap
from operator import add
import matplotlib.pyplot as plt


PWD    = os.path.dirname(os.path.abspath(__file__))

#-------------------------------------------------------------------------------

def gen_graph():

    enmd_avg_md_prep_times = []
    enmd_avg_md_times = []
    enmd_avg_exchange_prep_times = []
    enmd_avg_exchange_times = []
    enmd_avg_post_processing_times = []

    repex_avg_md_prep_times = []
    repex_avg_md_times = []
    repex_avg_exchange_prep_times = []
    repex_avg_exchange_times = []
    repex_avg_post_processing_times = []

    ############################################################################################
    # cycle 1

    md_prep_times = [19.918801]
    enmd_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [334.309237]
    enmd_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [27.234388]
    enmd_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )


    exchange_times = [283.840495]
    enmd_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [1.767561]
    enmd_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################
    #  

    md_prep_times = [2.483988]
    repex_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [327.670985]
    repex_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [4.789563]
    repex_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )

    exchange_times = [273.484081]
    repex_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [1.736008]
    repex_avg_post_processing_times.append( numpy.average(post_processing_times) )


    ############################################################################################

    ############################################################################################
    # cycle 2


    md_prep_times = [36.326203]
    enmd_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [365.021087]
    enmd_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [42.658023]
    enmd_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )


    exchange_times = [314.910602]
    enmd_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [2.352877]
    enmd_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################
    # 

    md_prep_times = [6.999114]
    repex_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [391.107183]
    repex_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [6.216651]
    repex_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )


    exchange_times = [335.301228]
    repex_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [2.610177]
    repex_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################

    ############################################################################################
    # cycle 3


    md_prep_times = [51.558342]
    enmd_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [458.8351]
    enmd_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [57.277029]
    enmd_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )


    exchange_times = [379.135106]
    enmd_avg_exchange_times.append( numpy.average(exchange_times) )
   
    post_processing_times = [3.338892]
    enmd_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################

    md_prep_times = [7.776671]
    repex_avg_md_prep_times.append( numpy.average(md_prep_times) )
    
    md_times = [440.809236]
    repex_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [6.14771]
    repex_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )

    exchange_times = [435.140356]
    repex_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [3.110729]
    repex_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################

    ############################################################################################
    # cycle 4

    md_prep_times = [66.329382]
    enmd_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [539.131452]
    enmd_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [71.436572]
    enmd_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )

    exchange_times = [386.685154]
    enmd_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [3.950811]
    enmd_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################

    md_prep_times = [9.867087]
    repex_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [541.274238]
    repex_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [8.275798]
    repex_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )

    exchange_times = [477.766798]
    repex_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [4.178001]
    repex_avg_post_processing_times.append( numpy.average(post_processing_times) )


    ############################################################################################

    ############################################################################################

    enmd_avg_md_prep_times = enmd_avg_md_prep_times[::-1]
    enmd_avg_md_times = enmd_avg_md_times[::-1]
    enmd_avg_exchange_prep_times = enmd_avg_exchange_prep_times[::-1]
    enmd_avg_exchange_times = enmd_avg_exchange_times[::-1]
    enmd_avg_post_processing_times = enmd_avg_post_processing_times[::-1]


    times_1 = map(add, enmd_avg_md_prep_times, enmd_avg_md_times)
    times_2 = map(add, enmd_avg_exchange_prep_times, enmd_avg_exchange_times)
    times_3 = map(add, times_1, times_2)

    enmd_total_times = map(add, times_3, enmd_avg_post_processing_times)

    repex_avg_md_prep_times = repex_avg_md_prep_times[::-1]
    repex_avg_md_times = repex_avg_md_times[::-1]
    repex_avg_exchange_prep_times = repex_avg_exchange_prep_times[::-1]
    repex_avg_exchange_times = repex_avg_exchange_times[::-1]
    repex_avg_post_processing_times = repex_avg_post_processing_times[::-1]


    times_1 = map(add, repex_avg_md_prep_times, repex_avg_md_times)
    times_2 = map(add, repex_avg_exchange_prep_times, repex_avg_exchange_times)
    times_3 = map(add, times_1, times_2)

    repex_total_times = map(add, times_3, repex_avg_post_processing_times)


    # Five subplots, the axes array is 1-d
    f, axarr = plt.subplots(6, sharex=True)

    N = 4

    ind = numpy.arange(N)    # the x locations for the groups
    width = 0.25             # the width of the bars: can also be len(x) sequence 
    plt.rc("font", size=8)


    p1_1 = axarr[0].bar(ind-0.5*width, enmd_avg_md_prep_times, width, color='sage', edgecolor = "white")
    p1_2 = axarr[0].bar(ind+0.5*width, repex_avg_md_prep_times, width, color='steelblue', edgecolor = "white")

    p2_1 = axarr[1].bar(ind-0.5*width, enmd_avg_md_times, width, color='sage', edgecolor = "white" )
    p2_2 = axarr[1].bar(ind+0.5*width, repex_avg_md_times, width, color='steelblue', edgecolor = "white")

    p3_1 = axarr[2].bar(ind-0.5*width, enmd_avg_exchange_prep_times, width, color='sage', edgecolor = "white" )
    p3_2 = axarr[2].bar(ind+0.5*width, repex_avg_exchange_prep_times, width, color='steelblue', edgecolor = "white")

    p4_1 = axarr[3].bar(ind-0.5*width, enmd_avg_exchange_times, width, color='sage', edgecolor = "white" )
    p4_2 = axarr[3].bar(ind+0.5*width, repex_avg_exchange_times, width, color='steelblue', edgecolor = "white")

    p5_1 = axarr[4].bar(ind-0.5*width, enmd_avg_post_processing_times, width, color='sage', edgecolor = "white" )
    p5_2 = axarr[4].bar(ind+0.5*width, repex_avg_post_processing_times, width, color='steelblue', edgecolor = "white" )

    p6_1 = axarr[5].bar(ind-0.5*width, enmd_total_times, width, color='darkolivegreen', edgecolor = "white" )
    p6_2 = axarr[5].bar(ind+0.5*width, repex_total_times, width, color='midnightblue', edgecolor = "white" )


    ############################

    axarr[0].set_ylabel('MD prep times')
    axarr[1].set_ylabel('MD times')
    axarr[2].set_ylabel('EX prep times')
    axarr[3].set_ylabel('EX times')
    axarr[4].set_ylabel('Post Proc times')
    axarr[5].set_ylabel('Total times')

    text ='Temperature-Exchange usecase (CDI) with NAMD on Tretsles; pattern B; Pilot size: 128 cores; Number of Replicas: 256'
    axarr[0].set_title('\n'.join(wrap(text,90)))
    
    plt.xlim(-0.25, 4.5)
    plt.xticks(ind+width/2., ('cycle-4', 'cycle-3', 'cycle-2', 'cycle-1' ) )
    
    plt.xlabel('Cycle Nr.')

    axarr[0].set_yticks(numpy.arange(0,120,20))
    axarr[1].set_yticks(numpy.arange(0,600,100))
    axarr[2].set_yticks(numpy.arange(0,120,20))
    axarr[3].set_yticks(numpy.arange(0,600,100))
    axarr[4].set_yticks(numpy.arange(0,8,1))
    axarr[5].set_yticks(numpy.arange(0,1250,250))

    plt.legend((p6_1[0], p6_2[0]), ('ENMD times', 'REPEX times') )

    axarr[0].yaxis.grid(True, which='major')
    axarr[1].yaxis.grid(True, which='major')
    axarr[2].yaxis.grid(True, which='major')
    axarr[3].yaxis.grid(True, which='major')
    axarr[4].yaxis.grid(True, which='major')
    axarr[5].yaxis.grid(True, which='major')
    
    
    plt.savefig('trestles-128-256-many-cycles.png')
   

#-------------------------------------------------------------------------------

if __name__ == "__main__":
    
    gen_graph()
