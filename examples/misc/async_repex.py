#!/usr/bin/env python

import radical.entk  as re
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def void():
    # entk needs callables as post_exec conditionals, even if there is nothing
    # to decide...
    pass


# ------------------------------------------------------------------------------
#
class Exchange(re.AppManager):

    _glyphs = {re.states.INITIAL:    'I',
               re.states.SCHEDULING: '!',
               re.states.SUSPENDED:  'X',
               re.states.DONE:       '=',
               re.states.FAILED:     '!',
               re.states.CANCELED:   '/'}

    def __init__(self, size, max_wait, min_cycles):

        self._size       = size
        self._max_wait   = max_wait
        self._min_cycles = min_cycles

        self._log = ru.Logger('radical.repex.exc')

        re.AppManager.__init__(self, autoterminate=False, port=5672) 
        self.resource_desc = {"resource" : 'local.localhost',
                              "walltime" : 30,
                              "cpus"     : 4}                                

        self._replicas  = list()
        self._waitlist  = list()

        # create the required number of replicas
        for i in range(size):

            replica = Replica(check_ex  = self._check_exchange,
                              check_res = self._check_resume,
                              rid       = i)

            self._replicas.append(replica)

        self._dump()

        # run the replica pipelines
        self.workflow = set(self._replicas)
        self.run() 


    # --------------------------------------------------------------------------
    #
    def get_replica(self, rid):

        for r in self._replicas:
            if r.rid == rid:
                return r
            
        return None


    # --------------------------------------------------------------------------
    #
    def _dump(self):

        with open('dump.log', 'a') as fout:
            fout.write('  ')
            for r in self._replicas:
                fout.write('%s ' % self._glyphs[r.state])
            fout.write('\n')
            fout.flush()


    # --------------------------------------------------------------------------
    #
    def terminate(self):

        self._dump()
        self._log.debug('exc term')

        # we are done!
        self.resource_terminate()


    # --------------------------------------------------------------------------
    #
    def _check_exchange(self, replica):

        self._dump()

        self._log.debug('=== %s check exchange : %d >= %d?',
                  replica.rid, len(self._waitlist), self._max_wait)

        self._waitlist.append(replica)

        if len(self._waitlist) < self._max_wait:

            # just suspend this replica and wait for the next
            self._log.debug('=== %s no  - suspend', replica.rid)
            replica.suspend()

        else:
            # before we continue, we need to make sure that any other
            # pipeline fills a *new* waitlist.  At the same time, we need to
            # tell *this* replica, what replicas need resuming after the
            # exchange
            #
            # NOTE: there is a race from the above append to this reset which
            #       should be guarded by a lock.  Does that need to be a process
            #       lock because EnTK spans processes?
            replica.set_suspended([r.rid for r in self._waitlist])
            self._waitlist = list()

            # we are in for a wild ride!
            self._log.debug('=== %s yes - exchange', replica.rid)

            task = re.Task()
            task.name       = 'extsk'
            task.executable = 'date'

            stage = re.Stage()
            stage.add_tasks(task)
            stage.post_exec = replica.check_resume

            replica.add_stages(stage)


    # --------------------------------------------------------------------------
    #
    def _check_resume(self, replica):

        self._dump()
        self._log.debug('=== %s check resume', replica.rid)

        resumed = list()  # list of resumed replica IDs

        # after a successfull exchange we revive all participating replicas
        # (whose rids we get with `replica.get_suspended()`).
        # For those replicas which did not yet reach min cycles, add an md
        # stage, all others we let die and add a new md stage for them.
        for rid in replica.get_suspended():

            _replica = self.get_replica(rid)

            if _replica.cycle <= self._min_cycles:
                _replica.add_md_stage()

            # Make sure we don't resume the current replica
            if replica.rid != _replica.rid:

                self._log.debug('=== %s resume', _replica.rid)
                _replica.resume()
                resumed.append(_replica.uid)

        # reset suspended list
        replica.set_suspended()

        return resumed


# ------------------------------------------------------------------------------
#
class Replica(re.Pipeline):
    '''
    A `Replica` is an EnTK pipeline which consists of alternating md and
    exchange stages.  The initial setup is for one MD stage - Exchange and more
    MD stages get added depending on runtime conditions.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, check_ex, check_res, rid):

        self._check_ex  = check_ex
        self._check_res = check_res
        self._rid       = rid

        self._cycle     = 0     # initial cycle
        self._suspended = None  # replicas to resume after exchange

        re.Pipeline.__init__(self)
        self.name = 'p_%s' % self.rid
        self._log = ru.Logger('radical.repex.rep')

        # add an initial md stage
        self.add_md_stage()


    @property
    def rid(self):      return self._rid

    @property
    def cycle(self):    return self._cycle


    # --------------------------------------------------------------------------
    #
    def set_suspended(self, rids=None):

        self._suspended = rids


    # --------------------------------------------------------------------------
    #
    def get_suspended(self):

        return self._suspended


    # --------------------------------------------------------------------------
    #
    def add_md_stage(self):

        self._log.debug('=== %s add md', self.rid)

        task = re.Task()
        task.name        = 'mdtsk-%s-%s' % (self.rid, self.cycle)
        task.executable  = 'date'

        stage = re.Stage()
        stage.add_tasks(task)
        stage.post_exec = self.check_exchange

        self.add_stages(stage)


    # --------------------------------------------------------------------------
    #
    def check_exchange(self):
        '''
        after an md cycle, record its completion and check for exchange
        '''

        self._cycle += 1
        self._check_ex(self)


    # --------------------------------------------------------------------------
    #
    def check_resume(self):
        '''
        after an ex cycle, trigger replica resumption
        '''
        return self._check_res(self)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    exchange = Exchange(size=16, max_wait=4, min_cycles=8)
    exchange.terminate()

# ------------------------------------------------------------------------------

