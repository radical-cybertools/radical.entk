import uuid
import json
import pika


def sync_with_master(obj, obj_type, channel, queue, logger, local_prof):

    object_as_dict = {'object': obj.to_dict()}
    if obj_type == 'Task':
        object_as_dict['type'] = 'Task'

    elif obj_type == 'Stage':
        object_as_dict['type'] = 'Stage'

    elif obj_type == 'Pipeline':
        object_as_dict['type'] = 'Pipeline'

    corr_id = str(uuid.uuid4())

    logger.debug('Attempting to sync %s with state %s with AppManager' % (obj.uid, obj.state))
    channel.basic_publish(exchange='',
                          routing_key=queue,
                          body=json.dumps(object_as_dict),
                          properties=pika.BasicProperties(correlation_id=corr_id)
                          )

    if obj_type == 'Task':
        local_prof.prof('publishing obj with state %s for sync' % obj.state, uid=obj.uid, msg=obj.parent_stage['uid'])
    elif obj_type == 'Stage':
        local_prof.prof('publishing obj with state %s for sync' %
                        obj.state, uid=obj.uid, msg=obj.parent_pipeline['uid'])
    else:
        local_prof.prof('publishing obj with state %s for sync' % obj.state, uid=obj.uid)

    sid = '-'.join(queue.split('-')[:-3])
    qname = queue.split('-')[-3:]
    qname.reverse()
    reply_queue = '-'.join(qname)
    reply_queue = sid + '-' + reply_queue

    while True:
        #self._logger.info('waiting for ack')

        method_frame, props, body = channel.basic_get(queue=reply_queue)

        if body:
            if corr_id == props.correlation_id:

                # print 'acknowledged: ', obj.uid, obj.state
                if obj_type == 'Task':
                    local_prof.prof('obj with state %s synchronized' %
                                    obj.state, uid=obj.uid, msg=obj.parent_stage['uid'])
                elif obj_type == 'Stage':
                    local_prof.prof('obj with state %s synchronized' %
                                    obj.state, uid=obj.uid, msg=obj.parent_pipeline['uid'])
                else:
                    local_prof.prof('obj with state %s synchronized' % obj.state, uid=obj.uid)

                logger.debug('%s with state %s synced with AppManager' % (obj.uid, obj.state))

                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                break
