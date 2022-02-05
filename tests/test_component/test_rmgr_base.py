# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase

from radical.entk.execman.base   import Base_ResourceManager as Rmgr
from radical.entk.exceptions import EnTKError

from hypothesis import given

import hypothesis.strategies as st

try:
    import mock
except ImportError:
    from unittest import mock


class TestBase(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='rmgr.0000')
    @mock.patch('os.getcwd', return_value='test_folder')
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_init(self, mocked_generate_id, mocked_getcwd, mocked_Logger,
                  mocked_Profiler):

        rmgr = Rmgr({'resource':'localhost'}, 'test_rmgr', 'rp', 'test_config')

        self.assertEqual(rmgr._resource_desc, {'resource':'localhost'})
        self.assertEqual(rmgr._sid, 'test_rmgr')
        self.assertEqual(rmgr._rts, 'rp')
        self.assertEqual(rmgr._rts_config, 'test_config')
        self.assertIsNone(rmgr._resource)
        self.assertIsNone(rmgr._walltime)
        self.assertEqual(rmgr._cpus, 1)
        self.assertEqual(rmgr._memory, 0)
        self.assertEqual(rmgr._gpus, 0)
        self.assertIsNone(rmgr._project)
        self.assertIsNone(rmgr._access_schema)
        self.assertIsNone(rmgr._queue)
        self.assertFalse(rmgr._validated)
        self.assertEqual(rmgr._uid, 'rmgr.0000')
        self.assertEqual(rmgr._path, 'test_folder/test_rmgr')
        self.assertIsInstance(rmgr._shared_data, list)
        self.assertIsNone(rmgr._job_name)
        self.assertIsNone(rmgr._outputs)

        with self.assertRaises(TypeError):
            rmgr = Rmgr('localhost', 'test_rmgr', 'rp', 'test_config')


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Rmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    @given(res_descr=st.fixed_dictionaries({'resource': st.text(),
                                            'walltime': st.integers(),
                                            'cpus': st.integers(),
                                            'gpus': st.integers(),
                                            'memory':st.integers(),
                                            'project': st.text(),
                                            'access_schema': st.text(),
                                            'queue': st.text(),
                                            'job_name': st.text()}))
    def test_validate(self, mocked_init, mocked_Logger, mocked_Profiler,
                      res_descr):

        rmgr = Rmgr({'resource':'localhost'}, 'test_rmgr', 'rp', 'test_config')

        rmgr._resource_desc = res_descr
        rmgr._rts_config = {'rts': 'some_rts'}
        rmgr._logger = mocked_Logger
        rmgr._prof = mocked_Profiler
        rmgr._uid = 'rmgr.0000'

        self.assertTrue(rmgr._validate_resource_desc())
        self.assertTrue(rmgr._validated)

        rmgr._resource_desc = {'resource' : 'localhost',
                               'walltime' : 30,
                               'cpus' : 1,
                               'gpus' : 1,
                               'project' : 'some_project',
                               'access_schema' : 'access',
                               'queue' : 'queue'}
        rmgr._rts_config = None

        with self.assertRaises(TypeError):
            rmgr._validate_resource_desc()

# ------------------------------------------------------------------------------
    #
    @mock.patch.object(Rmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    @given(res_descr=st.fixed_dictionaries({'resource': st.text(),
                                            'walltime': st.integers(),
                                            'cpus': st.integers(),
                                            'gpus': st.integers(),
                                            'memory':st.integers(),
                                            'project': st.text(),
                                            'access_schema': st.text(),
                                            'queue': st.text()}))
    def test_populate(self, mocked_init, mocked_Logger, mocked_Profiler,
                      res_descr):

        rmgr = Rmgr({'resource':'localhost'}, 'test_rmgr', 'rp', 'test_config')
        rmgr._validated = False
        rmgr._resource_desc = res_descr
        rmgr._logger = mocked_Logger
        rmgr._prof = mocked_Profiler
        rmgr._uid = 'rmgr.0000'

        with self.assertRaises(EnTKError):
            rmgr._populate()

        rmgr._validated = True
        rmgr._resource_desc = res_descr
        rmgr._logger = mocked_Logger
        rmgr._prof = mocked_Profiler
        rmgr._uid = 'rmgr.0000'

        rmgr._populate()
        self.assertEqual(rmgr._resource, res_descr['resource'])
        self.assertEqual(rmgr._walltime, res_descr['walltime'])
        self.assertEqual(rmgr._cpus, res_descr['cpus'])
        self.assertEqual(rmgr._gpus, res_descr['gpus'])
        self.assertEqual(rmgr._gpus, res_descr['memory'])
        self.assertEqual(rmgr._project, res_descr['project'])
        self.assertEqual(rmgr._access_schema, res_descr['access_schema'])
        self.assertEqual(rmgr._queue, res_descr['queue'])
