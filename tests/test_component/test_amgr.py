# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest   import TestCase
from hypothesis import given

import hypothesis.strategies as st

try:
    import mock
except ImportError:
    from unittest import mock

import radical.utils as ru
# import radical.entk.exceptions as ree

from radical.entk import AppManager as Amgr


class TestBase(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Amgr, '__init__', return_value=None)
    @mock.patch('pika.PlainCredentials')
    @mock.patch('pika.connection.ConnectionParameters')
    @given(d=st.fixed_dictionaries({'hostname': st.text(),
                                    'port': st.integers(),
                                    'username': st.text(),
                                    'password': st.text(),
                                    'reattempts': st.integers(),
                                    'resubmit_failed': st.booleans(),
                                    'autoterminate': st.booleans(),
                                    'write_workflow': st.booleans(),
                                    'rmq_cleanup': st.booleans(),
                                    'pending_qs' : st.integers(),
                                    'completed_qs' : st.integers()}))
    def test_amgr_read_config(self, mocked_init, mocked_PlainCredentials,
                              mocked_ConnectionParameters, d):

        amgr = Amgr(hostname='host', port='port',
                    username='username', password='password')

        d["rts"]        = "mock"
        d["rts_config"] = {"sandbox_cleanup": True,
                           "db_cleanup"     : True}

        ru.write_json(d, './config.json')
        amgr._read_config(config_path='./',
                          hostname=None,
                          port=None,
                          username=None,
                          password=None,
                          reattempts=None,
                          resubmit_failed=None,
                          autoterminate=None,
                          write_workflow=None,
                          rts=None,
                          rmq_cleanup=None,
                          rts_config=None)

        self.assertEqual(amgr._hostname ,d['hostname'])
        self.assertEqual(amgr._port ,d['port'])
        self.assertEqual(amgr._reattempts ,d['reattempts'])
        self.assertEqual(amgr._resubmit_failed ,d['resubmit_failed'])
        self.assertEqual(amgr._autoterminate ,d['autoterminate'])
        self.assertEqual(amgr._write_workflow ,d['write_workflow'])
        self.assertEqual(amgr._rts ,d['rts'])
        self.assertEqual(amgr._rts_config ,d['rts_config'])
        self.assertEqual(amgr._num_pending_qs ,d['pending_qs'])
        self.assertEqual(amgr._num_completed_qs ,d['completed_qs'])
        self.assertEqual(amgr._rmq_cleanup ,d['rmq_cleanup'])

        d['rts'] = 'another'
        ru.write_json(d, './config.json')
        print(d)
        with self.assertRaises(ValueError):
            amgr._read_config(config_path='./',
                              hostname=None,
                              port=None,
                              username=None,
                              password=None,
                              reattempts=None,
                              resubmit_failed=None,
                              autoterminate=None,
                              write_workflow=None,
                              rts=None,
                              rmq_cleanup=None,
                              rts_config=None)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Amgr, '__init__', return_value=None)
    @mock.patch('pika.PlainCredentials')
    @mock.patch('pika.connection.ConnectionParameters')
    @given(d2=st.fixed_dictionaries({'hostname': st.text(),
                                     'port': st.integers(),
                                     'username': st.text(),
                                     'password': st.text(),
                                     'reattempts': st.integers(),
                                     'resubmit_failed': st.booleans(),
                                     'autoterminate': st.booleans(),
                                     'write_workflow': st.booleans(),
                                     'rmq_cleanup': st.booleans(),
                                     'pending_qs' : st.integers(),
                                     'completed_qs' : st.integers()}))
    def test_amgr_read_config2(self, mocked_init, mocked_PlainCredentials,
                               mocked_ConnectionParameters, d2):

        amgr = Amgr(hostname='host', port='port',
                    username='username', password='password')

        amgr._read_config(config_path='./',
                          hostname=d2['hostname'],
                          port=d2['port'],
                          username=d2['username'],
                          password=d2['password'],
                          reattempts=d2['reattempts'],
                          resubmit_failed=d2['resubmit_failed'],
                          autoterminate=d2['autoterminate'],
                          write_workflow=d2['write_workflow'],
                          rmq_cleanup=d2['rmq_cleanup'],
                          rts="mock",
                          rts_config={"sandbox_cleanup": True,
                                      "db_cleanup"     : True})

        self.assertEqual(amgr._hostname, d2['hostname'])
        self.assertEqual(amgr._port, d2['port'])
        self.assertEqual(amgr._reattempts, d2['reattempts'])
        self.assertEqual(amgr._resubmit_failed, d2['resubmit_failed'])
        self.assertEqual(amgr._autoterminate, d2['autoterminate'])
        self.assertEqual(amgr._write_workflow, d2['write_workflow'])
        self.assertEqual(amgr._rts, "mock")
        self.assertEqual(amgr._rts_config, {"sandbox_cleanup": True,
                                            "db_cleanup"     : True})
        self.assertEqual(amgr._rmq_cleanup, d2['rmq_cleanup'])


    # --------------------------------------------------------------------------
    #
