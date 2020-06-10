#!/usr/bin/env python

import time
import random

import threading as mt

import radical.entk  as re
import radical.utils as ru

t_0 = time.time()

# ------------------------------------------------------------------------------
#
class ReplicaExchange(re.AppManager):

    _glyphs = {re.states.INITIAL:    '+',
               re.states.SCHEDULING: '|',
               re.states.SUSPENDED:  '-',
               re.states.DONE:       ' ',
               re.states.FAILED:     '!',
               re.states.CANCELED:   'X'}


    # --------------------------------------------------------------------------
    #
    def __init__(self, ensemble_size, exchange_size, md_cycles):

        self._en_size = ensemble_size
        self._ex_size = exchange_size
        self._cycles  = md_cycles

        self._lock = mt.Lock()
        self._log  = ru.Logger('radical.repex.exc')
        self._dout = open('dump.log', 'a')

        re.AppManager.__init__(self, autoterminate=False, port=5672)
        self.resource_desc = {"resource" : 'local.localhost',
                              "walltime" : 30,
                              "cpus"     : 16}

        self._replicas = list()
        self._waitlist = list()

        # create the required number of replicas
        for i in range(self._en_size):

            replica = Replica(check_ex  = self._check_exchange,
                              check_res = self._check_resume,
                              rid       = i)

            self._replicas.append(replica)

        self._dump(msg='startup')

        # run the replica pipelines
        self.workflow = set(self._replicas)
        self.run()


    # --------------------------------------------------------------------------
    #
    def _dump(self, msg=None, special=None, glyph=None ):

        if not msg:
            msg = ''

        self._dout.write(' | %7.2f |' % (time.time() - t_0))
        for r in self._replicas:
            if special and r in special:
                self._dout.write('%s' % glyph)
            else:
                self._dout.write('%s' % self._glyphs[r.state])
        self._dout.write('| %s\n' % msg)
        self._dout.flush()


    # --------------------------------------------------------------------------
    #
    def terminate(self):

        self._log.debug('exc term')
        self._dump(msg='terminate', special=self._replicas, glyph='=')
        self._dout.close()

        # we are done!
        self.resource_terminate()


    # --------------------------------------------------------------------------
    #
    def _check_exchange(self, replica):

        # This method races when concurrently triggered by multpiple replicas,
        # and it should be guarded by a lock.
        with self._lock:

            self._log.debug('=== %s check exchange : %d >= %d?',
                      replica.rid, len(self._waitlist), self._ex_size)

            self._waitlist.append(replica)

            exchange_list = self._find_exchange_list()

            if not exchange_list:
                # nothing to do, Suspend this replica and wait until we get more
                # candidates and can try again
                self._log.debug('=== %s no  - suspend', replica.rid)
                replica.suspend()
                self._dump()
                return

            self._log.debug('=== %s yes - exchange', replica.rid)
            msg = " > %s: %s" % (replica.rid, [r.rid for r in exchange_list])
            self._dump(msg=msg, special=exchange_list, glyph='v')

            # we have a set of exchange candidates.  The current replica is
            # tasked to host the exchange task.
            replica.add_ex_stage(exchange_list)


    # --------------------------------------------------------------------------
    #
    def _find_exchange_list(self):
        '''
        This is the core algorithm: out of the list of eligible replicas, select
        those which should be part of an exchange step
        '''

        if len(self._waitlist) < self._ex_size:

            # not enough replicas to attempt exchange
            return

        # we have enough replicas!  Remove all as echange candidates from the
        # waitlist and return them!
        exchange_list = list()
        for r in self._waitlist:
            exchange_list.append(r)

        # empty the waitlist to start collecting new cccadidates
        self._waitlist = list()

        return exchange_list


    # --------------------------------------------------------------------------
    #
    def _check_resume(self, replica):

        self._dump()
        self._log.debug('=== %s check resume', replica.rid)

        resumed = list()  # list of resumed replica IDs

        msg = " < %s: %s" % (replica.rid, [r.rid for r in replica.exchange_list])
        self._dump(msg=msg, special=replica.exchange_list, glyph='^')

        # after a successfull exchange we revive all participating replicas.
        # For those replicas which did not yet reach min cycles, add an md
        # stage, all others we let die and add a new md stage for them.
        for _replica in replica.exchange_list:

            if _replica.cycle <= self._cycles:
                _replica.add_md_stage()

            # Make sure we don't resume the current replica
            if replica.rid != _replica.rid:

                self._log.debug('=== %s resume', _replica.rid)
                _replica.resume()
                resumed.append(_replica.uid)

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
        self._ex_list   = None  # list of replicas used in exchange step

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
    @property
    def exchange_list(self):

        return self._ex_list


    # --------------------------------------------------------------------------
    #
    def add_md_stage(self):

        self._log.debug('=== %s add md', self.rid)

        task = re.Task()
        task.name        = 'mdtsk-%s-%s' % (self.rid, self.cycle)
        task.executable  = 'sleep'
        task.arguments   = [str(random.randint(0, 20)/10.0)]

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
    def add_ex_stage(self, exchange_list):

        self._log.debug('=== %s add ex: %s', self.rid,
                                             [r.rid for r in exchange_list])
        self._ex_list = exchange_list

        task = re.Task()
        task.name       = 'extsk'
        task.executable = 'date'

        stage = re.Stage()
        stage.add_tasks(task)
        stage.post_exec = self.check_resume

        self.add_stages(stage)


    # --------------------------------------------------------------------------
    #
    def check_resume(self):
        '''
        after an ex cycle, trigger replica resumption
        '''
        self._log.debug('=== check resume %s', self.rid)
        return self._check_res(self)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    exchange = ReplicaExchange(ensemble_size=128, exchange_size=8, md_cycles=16)
    exchange.terminate()

# ------------------------------------------------------------------------------

