import pandas as pd
import matplotlib.pyplot as plt


def plot_task_variation():

    df = pd.read_csv('monitor_task_variation.csv',skipinitialspace=True, index_col=['tasks'])
    df.drop(labels=['stages','pipelines'],inplace=True,axis=1)

    fig, ax = plt.subplots()
    ax1 = ax.twinx()

    df.cpu.plot(ax=ax,loglog=True, style='b-', marker='*')
    ax2 = df.memory.plot(ax=ax1,style='r-', marker='*', logy=True)

    ax.set_xlabel('No. of tasks per stage')
    ax.set_ylabel('CPU time (seconds)')
    ax2.set_ylabel('Memory (MB)')
    ax.set_title('CPU, Memory performance with varying number of task objects (stages per pipeline = 1, pipelines = 1)')


    l1 = ax.get_legend_handles_labels()
    l2 = ax2.get_legend_handles_labels()

    handles = l1[0] + l2[0]
    labels = l1[1] + l2[1]

    ax.legend(handles, labels, loc='upper left')

    #plt.savefig('plot_task_variation.pdf')
    plt.show()


def plot_stage_variation():

    df = pd.read_csv('monitor_stage_variation.csv',skipinitialspace=True, index_col=['stages'])
    df.drop(labels=['tasks','pipelines'],inplace=True,axis=1)

    fig, ax = plt.subplots()
    ax1 = ax.twinx()

    df.cpu.plot(ax=ax,loglog=True, style='b-', marker='*')
    ax2 = df.memory.plot(ax=ax1,style='r-', marker='*', logy=True)

    ax.set_xlabel('No. of stages per pipeline')
    ax.set_ylabel('CPU time (seconds)')
    ax2.set_ylabel('Memory (MB)')
    ax.set_title('CPU, Memory performance with varying number of stage objects (tasks per stage = 1000000, pipelines = 1)')


    l1 = ax.get_legend_handles_labels()
    l2 = ax2.get_legend_handles_labels()

    handles = l1[0] + l2[0]
    labels = l1[1] + l2[1]

    ax.legend(handles, labels, loc='upper left')

    #plt.savefig('plot_task_variation.pdf')
    plt.show()


def plot_pipeline_variation():

    df = pd.read_csv('monitor_pipeline_variation.csv',skipinitialspace=True, index_col=['pipelines'])
    df.drop(labels=['tasks','stages'],inplace=True,axis=1)

    fig, ax = plt.subplots()
    ax1 = ax.twinx()

    df.cpu.plot(ax=ax,loglog=True, style='b-', marker='*', xlim=(0,100000))
    ax2 = df.memory.plot(ax=ax1,style='r-', marker='*', logy=True)

    ax.set_xlabel('No. of pipelines')
    ax.set_ylabel('CPU time (seconds)')
    ax2.set_ylabel('Memory (MB)')
    ax.set_title('CPU, Memory performance with varying number of pipeline objects (tasks per stage = 1000, stages per pipeline = 1000000)')


    l1 = ax.get_legend_handles_labels()
    l2 = ax2.get_legend_handles_labels()

    handles = l1[0] + l2[0]
    labels = l1[1] + l2[1]

    ax.legend(handles, labels, loc='lower right')

    #plt.savefig('plot_task_variation.pdf')
    plt.show()


if __name__ == '__main__':

    plot_task_variation()
    plot_stage_variation()
    plot_pipeline_variation()
    