from radical.entk import AppManager, Pipeline, Stage, Task
from radical.entk.utils.sync_initiator import sync_with_master
import threading
import pika
import radical.utils as ru


if __name__ == '__main__':


    logger = ru.get_logger('radical.entk.temp_logger')
    amgr = AppManager()
    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    mq_channel = mq_connection.channel()

    # Setup the RabbitMQ system in order to test our synchronizer
    assert amgr._setup_mqs()

    p = Pipeline()
    s = Stage()

    # Create and add 100 tasks to the stage
    for cnt in range(100):

        t = Task()
        t.executable = ['some-executable-%s'%cnt]

        s.add_tasks(t)

    p.add_stages(s)

    amgr.assign_workflow(set([p]))

    # Start the synchronizer method in a thread
    sync_thread = Thread(target=self._synchronizer, name='synchronizer-thread')
    sync_thread.start()

    # Initialize ack counter
    acks_recv = 0

    for t in p.stages[0].tasks:

            sync_with_master(   obj=t, 
                                obj_type='Task', 
                                channel=mq_channel, 
                                reply_to='sync-ack-tmgr',
                                logger=logger, 
                                local_prof=profiler)