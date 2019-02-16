
from sync_initiator import sync_with_master


# ------------------------------------------------------------------------------
#
def transition(obj, obj_type, new_state, channel, queue, profiler, logger):

    try:
        old_state = obj.state
        obj.state = new_state

        if obj_type == 'Task':
            msg = obj.parent_stage['uid']
        elif obj_type == 'Stage':
            msg = obj.parent_pipeline['uid']
        else:
            msg = None

        profiler.prof('advance', uid=obj.uid, state=obj.state, msg=msg)

        sync_with_master(obj=obj,
                         obj_type=obj_type,
                         channel=channel,
                         queue=queue,
                         logger=logger,
                         local_prof=profiler)

        logger.info('Transition of %s to new state %s successful' % (obj.uid, new_state))


    except Exception:

        logger.exception('Transition of %s to %s state failed,',
                         obj.uid, new_state)

        # AM: why the reset if the transition failed?  Or is that
        # non-transactional after all?
        obj.state = old_state
        sync_with_master(obj=obj,
                         obj_type=obj_type,
                         channel=channel,
                         queue=queue,
                         logger=logger,
                         local_prof=profiler)
        raise

# ------------------------------------------------------------------------------

