import traceback
def func():
    try:
        try:
            1/0
        except Exception as ex:
            print 'caught error, ex: '
            print traceback.format_exc()
            raise
    except Exception, ex:
        print 'test, ex: '
        print traceback.format_exc()


func()
