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
    # 64/128 

    md_prep_times = [11.15554, 12.643986, 16.038453]
    enmd_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [119.813373, 139.695717, 156.084988]
    enmd_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [9.891328, 13.688966, 16.503259]
    enmd_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )


    exchange_times = [102.411035, 121.539652, 128.82382]
    enmd_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [0.671823, 0.798953, 1.097424]
    enmd_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################
    # OK

    md_prep_times = [1.33276, 2.948043, 3.450892]
    repex_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [137.878725, 156.24525, 174.764014]
    repex_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [2.171397, 2.699995, 2.915867]
    repex_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )

    exchange_times = [125.228446, 142.720571, 153.899415]
    repex_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [0.488691, 0.72613, 0.774859]
    repex_avg_post_processing_times.append( numpy.average(post_processing_times) )


    ############################################################################################

    ############################################################################################
    # 128/256

    md_prep_times = [22.541008, 35.884892]
    enmd_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [271.408067, 365.774492]
    enmd_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [26.389173, 41.275375]
    enmd_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )


    exchange_times = [252.021597, 305.552244]
    enmd_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [2.053707, 2.707205]
    enmd_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################
    # ok


    md_prep_times = [3.442241, 7.023973]
    repex_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [303.468326, 387.972735]
    repex_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [4.880221, 6.347701]
    repex_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )


    exchange_times = [283.058266, 334.289674]
    repex_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [1.738836, 2.537882]
    repex_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################

    ############################################################################################
    # 256/512 

    md_prep_times = [43.535514, 125.557439]
    enmd_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [649.177261, 1249.577718]
    enmd_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [78.872782, 153.390076]
    enmd_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )


    exchange_times = [820.879971, 1127.394562]
    enmd_avg_exchange_times.append( numpy.average(exchange_times) )
   
    post_processing_times = [10.086089, 11.759318]
    enmd_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################

    md_prep_times = [3.918126, 17.080726]
    repex_avg_md_prep_times.append( numpy.average(md_prep_times) )
    
    md_times = [765.13554, 1280.874559]
    repex_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [10.708313, 16.369491]
    repex_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )

    exchange_times = [912.782984, 1386.196337]
    repex_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [9.147824, 10.985776]
    repex_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################

    ############################################################################################
    # 384/768

    md_prep_times = [98.983471, 370.094089]
    enmd_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [1314.953057, 2386.687527]
    enmd_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [222.776623, 460.798372]
    enmd_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )

    exchange_times = [1870.395685, 2527.059035]
    enmd_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [24.110429, 22.896567]
    enmd_avg_post_processing_times.append( numpy.average(post_processing_times) )

    ############################################################################################
   
    md_prep_times = [6.488597, 33.02816]
    repex_avg_md_prep_times.append( numpy.average(md_prep_times) )
    

    md_times = [1553.938179, 2337.339562]
    repex_avg_md_times.append( numpy.average(md_times) )
 

    exchange_prep_times = [19.786654, 31.675783]
    repex_avg_exchange_prep_times.append( numpy.average(exchange_prep_times) )

    exchange_times = [1797.384527, 3057.211613]
    repex_avg_exchange_times.append( numpy.average(exchange_times) )
   

    post_processing_times = [18.692643, 24.879094]
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

    text ='Temperature-Exchange usecase (CDI) with NAMD on Tretsles; pattern B; variable Pilot size/Number of Replicas'
    axarr[0].set_title('\n'.join(wrap(text,90)))
    
    plt.xlim(-0.25, 4.5)
    plt.xticks(ind+width/2., ('384/768', '256/512', '128/256', '64/128' ) )
    
    plt.xlabel('Pilot size/Replicas')

    axarr[0].set_yticks(numpy.arange(0,250,50))
    axarr[1].set_yticks(numpy.arange(0,2500,300))
    axarr[2].set_yticks(numpy.arange(0,400,50))
    axarr[3].set_yticks(numpy.arange(0,2800,500))
    axarr[4].set_yticks(numpy.arange(0,30,5))
    axarr[5].set_yticks(numpy.arange(0,5000,750))

    plt.legend((p6_1[0], p6_2[0]), ('ENMD times', 'REPEX times') )

    axarr[0].yaxis.grid(True, which='major')
    axarr[1].yaxis.grid(True, which='major')
    axarr[2].yaxis.grid(True, which='major')
    axarr[3].yaxis.grid(True, which='major')
    axarr[4].yaxis.grid(True, which='major')
    axarr[5].yaxis.grid(True, which='major')
    
    
    plt.savefig('trestles-variable-pilots-variable-replicas.png')
   

#-------------------------------------------------------------------------------

if __name__ == "__main__":
    
    gen_graph()
