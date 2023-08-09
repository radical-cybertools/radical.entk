# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter, import-error

from unittest import TestCase
from random import shuffle

from   hypothesis import given, settings
import hypothesis.strategies as st

from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import EnTKTypeError, EnTKValueError, EnTKMissingError, EnTKError

try:
    import mock
except ImportError:
    from unittest import mock


# Hypothesis settings
settings.register_profile("travis", max_examples=100, deadline=None)
settings.load_profile("travis")


# ------------------------------------------------------------------------------
#
class TestBase(TestCase):

    @mock.patch('radical.utils.generate_id', return_value='pipeline.0000')
    @mock.patch('threading.Lock', return_value='test_lock')
    @mock.patch('threading.Event', return_value='test_event')
    def test_pipeline_initialization(self, mocked_generate_id, mocked_Lock,
                                     mocked_Event):

        p = Pipeline()

        self.assertEqual(p.uid, 'pipeline.0000')
        self.assertEqual(p.name, None)
        self.assertEqual(p.stages, list())
        self.assertEqual(p.state, states.INITIAL)
        self.assertEqual(p.state_history, [states.INITIAL])
        self.assertEqual(p._stage_count, 0)
        self.assertEqual(p.current_stage, 0)
        self.assertEqual(p._lock, 'test_lock')
        self.assertEqual(p._completed_flag, 'test_event')

        p._completed_flag = mock.Mock()
        p._completed_flag.is_set = mock.MagicMock(return_value=False)

        self.assertFalse(p.completed)


    # --------------------------------------------------------------------------
    #
    # @given(t=st.text(alphabet=string.ascii_letters +
    #                           string.punctuation.replace('.', ''),
    #                  min_size=10).filter(
    #     lambda x: any(symbol in x for symbol in string.punctuation)),
    @mock.patch('radical.utils.generate_id', return_value='pipeline.0000')
    @mock.patch('threading.Lock', return_value='test_lock')
    @mock.patch('threading.Event', return_value='test_event')
    @given(l=st.lists(st.text()),
        i=st.integers().filter(lambda x: type(x) == int),
        b=st.booleans(),
        se=st.sets(st.text()))
    def test_pipeline_assignment_exceptions(self, mocked_generate_id,
                                            mocked_Lock, mocked_Event, l, i,
                                            b, se):

        p = Pipeline()

        data_type = [l, i, b, se]
        for data in data_type:
            if not isinstance(data, str):
                with self.assertRaises(EnTKTypeError):
                    p.name = data

            if isinstance(data,str):
                with self.assertRaises(EnTKValueError):
                    p.name = data

            with self.assertRaises(EnTKTypeError):
                p.stages = data

            with self.assertRaises(EnTKTypeError):
                p.add_stages(data)

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='pipeline.0000')
    @mock.patch('threading.Lock', return_value='test_lock')
    @mock.patch('threading.Event', return_value='test_event')
    def test_pipeline_stage_assignment(self, mocked_generate_id, mocked_Lock,
                                       mocked_Event):

        p = Pipeline()
        s = mock.MagicMock(spec=Stage)
        p.stages = s

        self.assertEqual(type(p.stages), list)
        self.assertEqual(p._stage_count, 1)
        self.assertEqual(p._cur_stage, 1)
        self.assertEqual(p.stages[0], s)


    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='pipeline.0000')
    @mock.patch('threading.Lock', return_value='test_lock')
    @mock.patch('threading.Event', return_value='test_event')
    @given(t=st.text(),
        l=st.lists(st.text()),
        i=st.integers().filter(lambda x: type(x) == int),
        b=st.booleans())
    def test_pipeline_state_assignment(self, mocked_generate_id, mocked_Lock,
                                       mocked_Event, t, l, i, b):

        p = Pipeline()

        data_type = [l, i, b]

        for data in data_type:
            with self.assertRaises(EnTKTypeError):
                p.state = data

        if isinstance(t,str):
            with self.assertRaises(EnTKValueError):
                p.state = t

        state_history = list()
        p = Pipeline()
        p._state = None
        p._state_history = list()
        states_list = list(states._pipeline_state_values.keys())
        shuffle(states_list)
        for val in states_list:
            p.state = val
            if val != states.SUSPENDED:
                state_history.append(val)
            self.assertEqual(p._state, val)
            self.assertEqual(p._state_history, state_history)

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='pipeline.0000')
    @mock.patch('threading.Lock', return_value='test_lock')
    @mock.patch('threading.Event', return_value='test_event')
    def test_pipeline_stage_addition(self, mocked_generate_id, mocked_Lock,
                                     mocked_Event):

        p = Pipeline()
        s1 = mock.MagicMock(spec=Stage)
        s2 = mock.MagicMock(spec=Stage)
        p.add_stages([s1, s2])

        self.assertEqual(type(p.stages), list)
        self.assertEqual(p._stage_count, 2)
        self.assertEqual(p._cur_stage, 1)
        self.assertEqual(p.stages[0], s1)
        self.assertEqual(p.stages[1], s2)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_as_dict(self, mocked_init):

        p = Pipeline()
        p._uid  = 'pipeline.0000'
        p._name = 'test_pipeline'
        p._stages = list()
        p._state = states.INITIAL
        p._state_history = [states.INITIAL]
        p._stage_count = len(p._stages)
        p._cur_stage = 0
        p._completed_flag = mock.Mock()
        p._completed_flag.is_set = mock.MagicMock(return_value=False)

        d = p.as_dict()
        self.assertEqual(d, {'uid': 'pipeline.0000',
                             'name': 'test_pipeline',
                             'state': states.INITIAL,
                             'state_history': [states.INITIAL],
                             'completed': False})


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_from_dict(self, mocked_init):

        d = {'uid': 're.Pipeline.0000',
             'name': 'p1',
             'state': states.DONE,
             'state_history': [states.INITIAL, states.DONE],
             'completed': True}

        p = Pipeline()
        p._completed_flag = mock.Mock()
        p._completed_flag.is_set = mock.MagicMock(return_value=d['completed'])
        p.from_dict(d)

        self.assertEqual(p.uid, d['uid'])
        self.assertEqual(p.name, d['name'])
        self.assertEqual(p.state, d['state'])
        self.assertEqual(p.state_history, d['state_history'])
        self.assertEqual(p.completed, d['completed'])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_increment_stage(self, mocked_init):

        p = Pipeline()
        p._completed_flag = mock.Mock()
        p._completed_flag.is_set = mock.MagicMock(side_effect=[False, False, True])
        p._cur_stage = 0
        p._stage_count = 2
        p._increment_stage()

        self.assertEqual(p._stage_count, 2)
        self.assertEqual(p._cur_stage, 1)
        self.assertFalse(p._completed_flag.is_set())

        p._increment_stage()
        self.assertEqual(p._stage_count, 2)
        self.assertEqual(p._cur_stage, 2)
        self.assertFalse(p._completed_flag.is_set())

        p._increment_stage()
        self.assertEqual(p._stage_count, 2)
        self.assertEqual(p._cur_stage, 2)
        self.assertTrue(p._completed_flag.is_set())


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_decrement_stage(self, mocked_init):

        p = Pipeline()
        p._completed_flag = mock.Mock()
        p._completed_flag.is_set = mock.MagicMock(side_effect=[False, True])
        p._cur_stage = 2
        p._stage_count = 2

        p._decrement_stage()
        self.assertEqual(p._stage_count, 2)
        self.assertEqual(p._cur_stage, 1)
        self.assertFalse(p._completed_flag.is_set(), False)

        p._decrement_stage()
        self.assertEqual(p._stage_count, 2)
        self.assertEqual(p._cur_stage, 0)
        self.assertFalse(p._completed_flag.is_set(), False)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    @given(t=st.text(),
           l=st.lists(st.text()),
           i=st.integers().filter(lambda x: type(x) == int),
           b=st.booleans(),
           se=st.sets(st.text()))
    def test_pipeline_validate_entities(self, mocked_init,  t, l, i, b, se):

        p = Pipeline()

        data_type = [t, l, i, b, se]

        for data in data_type:
            with self.assertRaises(EnTKTypeError):
                p._validate_entities(data)

        s = mock.MagicMock(spec=Stage)
        self.assertIsInstance(p._validate_entities(s), list)

        s1 = mock.MagicMock(spec=Stage)
        s2 = mock.MagicMock(spec=Stage)
        self.assertEqual([s1,s2], p._validate_entities([s1,s2]))


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    @mock.patch.object(Stage, '_validate', return_value=None)
    @mock.patch.object(Task, '_validate', return_value=None)
    def test_pipeline_validate(self, mocked_t_validate, mocked_s_validate,
                                     mocked_init):

        p = Pipeline()
        p._uid   = 'pipeline.0000'
        p._state = 'test'
        with self.assertRaises(EnTKValueError):
            p._validate()

        p = Pipeline()
        p._uid    = 'pipeline.0000'
        p._stages = []
        p._state  = states.INITIAL
        with self.assertRaises(EnTKMissingError):
            p._validate()

        s = mock.MagicMock(spec=Stage)
        p = Pipeline()
        p._uid    = 'pipeline.0000'
        p._stages = [s]
        p._state  = states.INITIAL
        p._validate()

        # test annotations
        s1 = Stage()
        s2 = Stage()
        t1 = Task()
        t2 = Task()

        t1.annotate(outputs=['file_t1_1.txt', 'file_t1_2.txt'])
        t2.annotate(inputs={t1: ['not_produced_by_t1']})
        s1.add_tasks(t1)
        s2.add_tasks(t2)
        p._stages = [s1, s2]
        with self.assertRaises(EnTKError) as ee:
            # provided input as from task `t1`, which is not produced by `t1`
            p._validate()
        self.assertIn('Annotation error', str(ee.exception))

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_properties(self, mocked_init):

        p = Pipeline()
        p._name = 'test_pipe'
        self.assertEqual(p.name, 'test_pipe')
        self.assertEqual(p.luid, 'test_pipe')

        p = Pipeline()
        p._stages = ['test_stage']
        self.assertEqual(p.stages, ['test_stage'])

        p = Pipeline()
        p._state = 'test_state'
        self.assertEqual(p.state, 'test_state')

        p = Pipeline()
        p._uid = 'pipeline.0000'
        p._name = None
        self.assertEqual(p.uid, 'pipeline.0000')
        self.assertEqual(p.luid, 'pipeline.0000')

        p = Pipeline()
        p._lock = 'test_lock'
        self.assertEqual(p.lock, 'test_lock')

        p = Pipeline()
        p._completed_flag = mock.Mock()
        p._completed_flag.is_set = mock.MagicMock(return_value='flag_set')
        self.assertEqual(p.completed, 'flag_set')

        p = Pipeline()
        p._cur_stage = 'some_stage'
        self.assertEqual(p.current_stage, 'some_stage')

        p = Pipeline()
        p._state_history = ['state1','state2']
        self.assertEqual(p.state_history, ['state1','state2'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_suspend(self, mocked_init):

        p = Pipeline()
        p._state = states.SCHEDULED
        p._state_history = [states.SCHEDULED]
        p.suspend()
        self.assertEqual(p._state, states.SUSPENDED)
        self.assertEqual(p._state_history, [states.SCHEDULED, states.SUSPENDED])

        p = Pipeline()
        p._uid = 'pipeline.0000'
        p._state = states.SUSPENDED
        with self.assertRaises(EnTKError):
            p.suspend()

# ------------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_resume(self, mocked_init):

        p = Pipeline()
        p._state = states.SUSPENDED
        p._state_history = [states.SCHEDULED, states.SUSPENDED]
        p.resume()
        self.assertEqual(p._state, states.SCHEDULED)
        self.assertEqual(p._state_history, [states.SCHEDULED, states.SUSPENDED,
                                    states.SCHEDULED])

        p = Pipeline()
        p._uid = 'pipeline.0000'
        p._state = states.SCHEDULED
        p._state_history = [states.SCHEDULED]
        with self.assertRaises(EnTKError):
            p.resume()
