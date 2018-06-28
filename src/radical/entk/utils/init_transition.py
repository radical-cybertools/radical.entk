from sync_initiator import sync_with_master


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

        profiler.prof('advance',
                      uid=obj.uid,
                      state=obj.state,
                      msg=msg)

        sync_with_master(obj=obj,
                         obj_type=obj_type,
                         channel=channel,
                         queue=queue,
                         logger=logger,
                         local_prof=profiler)

        logger.info('Transition of %s to new state %s successful' % (obj.uid, new_state))

    except Exception, ex:

        logger.exception('Transition of %s to %s state failed, error: %s' % (obj.uid, new_state, ex))
        obj.state = old_state
        sync_with_master(obj=obj,
                         obj_type=obj_type,
                         channel=channel,
                         queue=queue,
                         logger=logger,
                         local_prof=profiler)
        raise
