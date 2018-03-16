from radical.entk import AppManager, Pipeline, Stage, Task, states
import pytest
from radical.entk.exceptions import *
import pika
import radical.utils as ru
from threading import Event, Thread
from multiprocessing import Process
from radical.entk.utils.sync_initiator import sync_with_master
import os

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port = os.environ.get('RMQ_PORT',5672)

def test_sid_in_mqs():

    """
    **Purpose**: Test if all queues created in the RMQ server have a unique
    id derived from the session ID of the AppManager
    """

    appman = AppManager()
    appman._setup_mqs()
    sid = appman._sid

    qs =    [
                    '%s-tmgr-to-sync' % sid,
                    '%s-cb-to-sync' % sid,
                    '%s-enq-to-sync' % sid,
                    '%s-deq-to-sync' % sid,
                    '%s-sync-to-tmgr' % sid,
                    '%s-sync-to-cb' % sid,
                    '%s-sync-to-enq' % sid,
                    '%s-sync-to-deq' % sid
                ]

    mq_connection = pika.BlockingConnection(
                        pika.ConnectionParameters(
                            host=hostname,
                            port=port)
                        )
    mq_channel = mq_connection.channel()

    def callback():
        print True

    for q in qs:

        try:
            mq_channel.basic_consume(callback, queue=q, no_ack=True)
        except Exception as ex:
            raise Error(ex)


def func_for_process(sid, p, mq_channel, logger, profiler):

    try:

        mq_connection = pika.BlockingConnection(
                        pika.ConnectionParameters(
                            host=hostname,
                            port=port)
                        )
        mq_channel = mq_connection.channel()

        for t in p.stages[0].tasks:

            t.state = states.SCHEDULING
            sync_with_master(   obj=t, 
                                obj_type='Task', 
                                channel=mq_channel, 
                                queue='%s-tmgr-to-sync'%sid,
                                logger=logger, 
                                local_prof=profiler)

    except KeyboardInterrupt:
        raise KeyboardInterrupt

    except Exception, ex:
        raise

def test_synchronizer():


    logger = ru.get_logger('radical.entk.temp_logger')
    profiler = ru.Profiler(name = 'radical.entk.temp')
    amgr = AppManager()
    
    mq_connection = pika.BlockingConnection(
                        pika.ConnectionParameters(
                            host=hostname,
                            port=port)
                        )

    mq_channel = mq_connection.channel()

    amgr._setup_mqs()
    
    p = Pipeline()
    s = Stage()

    # Create and add 100 tasks to the stage
    for cnt in range(100):

        t = Task()
        t.executable = ['some-executable-%s'%cnt]

        s.add_tasks(t)

    p.add_stages(s)
    p._initialize(amgr._sid)

    amgr.assign_workflow(set([p]))

    for t in p.stages[0].tasks:
        assert t.state == states.INITIAL

    # Start the synchronizer method in a thread
    amgr._end_sync = Event()
    sync_thread = Thread(target=amgr._synchronizer, name='synchronizer-thread')
    sync_thread.start()        

    # Start the synchronizer method in a thread
    proc = Process(target=func_for_process, name='temp-proc',
                    args=(amgr._sid, p, mq_channel, logger, profiler))

    proc.start()
    proc.join()

    for t in p.stages[0].tasks:
        assert t.state == states.SCHEDULING

    amgr._end_sync.set()
    sync_thread.join()