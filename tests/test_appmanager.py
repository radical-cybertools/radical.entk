from radical.entk import AppManager
import pytest
from radical.entk.exceptions import *

def test_attribute_types():

    """
    **Purpose**: Test all AppManager attribute types exposed to the user
    """

    appman = AppManager()
    assert type(appman.name) == str

    data_type = [1,'a',True, [1], set([1])]

    for data in data_type:

        with pytest.raises(TypeError):
            appman.resource_manager = data


def test_assign_workload_exception():

    """
    **Purpose**: Test workload assignment and validation
    """

    appman = AppManager()
    data_type = [1,'a',True, [1], set([1])]

    for data in data_type:

        with pytest.raises(Error):
            appman.assign_workflow(data)


def test_sid_in_mqs():

    """
    **Purpose**: Test if all queues created in the RMQ server have a unique
    id derived from the session ID of the AppManager
    """

    import pika


    appman = AppManager()
    appman._setup_mqs()
    sid = appman._sid
    qnames = [  'pendingq-1', 'completedq-1',
                'tmgr-to-sync', 'cb-to-sync',
                'enq-to-sync', 'deq-to-sync',
                'sync-to-tmgr', 'sync-to-cb',
                'sync-to-enq', 'sync-to-deq']

    qs = ['%s-%s'%(sid, qname) for qname in qnames]

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters())
    mq_channel = mq_connection.channel()

    def callback():
        print True

    for q in qs:

        try:
            mq_channel.basic_consume(callback, queue=q, no_ack=True)
        except Exception as ex:
            raise Error(ex)