import uuid

def sync_with_master(obj, obj_type, channel, reply_to, logger, local_prof):

    object_as_dict = {'object': obj.to_dict()}
    if obj_type == 'Task': 
        object_as_dict['type'] = 'Task'

    elif obj_type == 'Stage':
        object_as_dict['type'] = 'Stage'

    elif obj_type == 'Pipeline':
        object_as_dict['type'] = 'Pipeline'

    corr_id = str(uuid.uuid4())

    channel.basic_publish(
                            exchange='',
                            routing_key='sync-to-master',
                            body=json.dumps(object_as_dict),
                            properties=pika.BasicProperties(
                                        reply_to = reply_to,
                                        correlation_id = corr_id
                                        )
                        )

    if obj_type == 'Task':
        local_prof.prof('publishing obj with state %s for sync'%obj.state, uid=obj.uid, msg=obj._parent_stage)
    elif obj_type == 'Stage':
        local_prof.prof('publishing obj with state %s for sync'%obj.state, uid=obj.uid, msg=obj._parent_pipeline)
    else:
        local_prof.prof('publishing obj with state %s for sync'%obj.state, uid=obj.uid)


                
    while True:
        #self._logger.info('waiting for ack')
        method_frame, props, body = channel.basic_get(queue=reply_to)

        if body:
            if corr_id == props.correlation_id:

                #print 'acknowledged: ', obj.uid, obj.state
                if obj_type == 'Task':
                    local_prof.prof('obj with state %s synchronized'%obj.state, uid=obj.uid, msg=obj._parent_stage)
                elif obj_type == 'Stage':
                    local_prof.prof('obj with state %s synchronized'%obj.state, uid=obj.uid, msg=obj._parent_pipeline)
                else:
                    local_prof.prof('obj with state %s synchronized'%obj.state, uid=obj.uid)
                            
                logger.info('%s synchronized'%obj.uid)

                channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                break