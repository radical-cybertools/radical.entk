# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

import pytest
from unittest import TestCase

from   hypothesis import given, settings
import hypothesis.strategies as st

from radical.entk import Pipeline, Stage
from radical.entk import states
from radical.entk.exceptions import TypeError, ValueError, MissingError, EnTKError

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

        assert p.uid == 'pipeline.0000'
        assert p.name is None
        assert p.stages == list()
        assert p.state == states.INITIAL
        assert p.state_history == [states.INITIAL]
        assert p._stage_count == 0
        assert p.current_stage == 0
        assert p._lock == 'test_lock'
        assert p._completed_flag == 'test_event'

        p._completed_flag = mock.Mock()
        p._completed_flag.is_set = mock.MagicMock(return_value=False)

        assert p.completed is False


    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='pipeline.0000')
    @mock.patch('threading.Lock', return_value='test_lock')
    @mock.patch('threading.Event', return_value='test_event')
    @given(t=st.text(),
        l=st.lists(st.text()),
        i=st.integers().filter(lambda x: type(x) == int),
        b=st.booleans(),
        se=st.sets(st.text()))
    def test_pipeline_assignment_exceptions(self, mocked_generate_id,
                                            mocked_Lock, mocked_Event, t, l, i,
                                            b, se):

        p = Pipeline()

        data_type = [t, l, i, b, se]

        for data in data_type:

            if not isinstance(data, str):
                with pytest.raises(TypeError):
                    p.name = data

            with pytest.raises(TypeError):
                p.stages = data

            with pytest.raises(TypeError):
                p.add_stages(data)


    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='pipeline.0000')
    @mock.patch('threading.Lock', return_value='test_lock')
    @mock.patch('threading.Event', return_value='test_event')
    def test_pipeline_stage_assignment(self, mocked_generate_id, mocked_Lock,
                                       mocked_Event):

        p = Pipeline()
        s = mock.MagicMock(spec=Stage)
        p.stages = s

        assert type(p.stages) == list
        assert p._stage_count == 1
        assert p._cur_stage == 1
        assert p.stages[0] == s


    # ------------------------------------------------------------------------------
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
            with pytest.raises(TypeError):
                p.state = data

        if isinstance(t,str):
            with pytest.raises(ValueError):
                p.state = t

        for val in list(states._pipeline_state_values.keys()):
            p.state = val


    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='pipeline.0000')
    @mock.patch('threading.Lock', return_value='test_lock')
    @mock.patch('threading.Event', return_value='test_event')
    def test_pipeline_stage_addition(self, mocked_generate_id, mocked_Lock,
                                     mocked_Event):

        # FIXME: instantiates and test also task addition for stages.
        p = Pipeline()
        s1 = mock.MagicMock(spec=Stage)
        s2 = mock.MagicMock(spec=Stage)
        p.add_stages([s1, s2])

        assert type(p.stages) == list
        assert p._stage_count == 2
        assert p._cur_stage == 1
        assert p.stages[0] == s1
        assert p.stages[1] == s2


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_to_dict(self, mocked_init):

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

        d = p.to_dict()
        assert d == {'uid': 'pipeline.0000',
                    'name': 'test_pipeline',
                    'state': states.INITIAL,
                    'state_history': [states.INITIAL],
                    'completed': False}


    # ------------------------------------------------------------------------------
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

        assert p.uid == d['uid']
        assert p.name == d['name']
        assert p.state == d['state']
        assert p.state_history == d['state_history']
        assert p.completed == d['completed']


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_increment_stage(self, mocked_init):

        p = Pipeline()
        p._completed_flag = mock.Mock()
        p._completed_flag.is_set = mock.MagicMock(side_effect=[False, False, True])
        p._cur_stage = 0
        p._stage_count = 2
        p._increment_stage()

        assert p._stage_count == 2
        assert p._cur_stage == 1
        assert p._completed_flag.is_set() is False

        p._increment_stage()
        assert p._stage_count == 2
        assert p._cur_stage == 2
        assert p._completed_flag.is_set() is False

        p._increment_stage()
        assert p._stage_count == 2
        assert p._cur_stage == 2
        assert p._completed_flag.is_set() is True


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_decrement_stage(self, mocked_init):

        p = Pipeline()
        p._completed_flag = mock.Mock()
        p._completed_flag.is_set = mock.MagicMock(side_effect=[False, True])
        p._cur_stage = 2
        p._stage_count = 2

        p._decrement_stage()
        assert p._stage_count == 2
        assert p._cur_stage == 1
        assert p._completed_flag.is_set() is False

        p._decrement_stage()
        assert p._stage_count == 2
        assert p._cur_stage == 0
        assert p._completed_flag.is_set() is False


    # ------------------------------------------------------------------------------
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
            with pytest.raises(TypeError):
                p._validate_entities(data)

        s = mock.MagicMock(spec=Stage)
        assert isinstance(p._validate_entities(s), list)

        s1 = mock.MagicMock(spec=Stage)
        s2 = mock.MagicMock(spec=Stage)
        assert [s1,s2] == p._validate_entities([s1,s2])


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_validate(self, mocked_init):

        p = Pipeline()
        p._uid = 'pipeline.0000'
        p._state = 'test'
        with pytest.raises(ValueError):
            p._validate()

        p = Pipeline()
        p._uid = 'pipeline.0000'
        p._stages = list()
        p._state = states.INITIAL
        with pytest.raises(MissingError):
            p._validate()

        p = Pipeline()
        p._uid = 'pipeline.0000'
        s = mock.MagicMock(spec=Stage)
        s._validate = mock.MagicMock(return_value=True)
        p._stages = [s]
        p._state = states.INITIAL
        p._validate()

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_properties(self, mocked_init):

        p = Pipeline()
        p._name = 'test_pipe'
        assert p.name == 'test_pipe'
        assert p.luid == 'test_pipe'

        p = Pipeline()
        p._stages = ['test_stage']
        assert p.stages == ['test_stage']

        p = Pipeline()
        p._state = 'test_state'
        assert p.state == 'test_state'

        p = Pipeline()
        p._uid = 'pipeline.0000'
        p._name = None
        assert p.uid == 'pipeline.0000'
        assert p.luid == 'pipeline.0000'

        p = Pipeline()
        p._lock = 'test_lock'
        assert p.lock == 'test_lock'

        p = Pipeline()
        p._completed_flag = mock.Mock()
        p._completed_flag.is_set = mock.MagicMock(return_value='flag_set')
        assert p.completed == 'flag_set'

        p = Pipeline()
        p._cur_stage = 'some_stage'
        assert p.current_stage == 'some_stage'

        p = Pipeline()
        p._state_history = ['state1','state2']
        assert p.state_history == ['state1','state2']

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_suspend(self, mocked_init):

        p = Pipeline()
        p._state = states.SCHEDULED
        p._state_history = [states.SCHEDULED]
        p.suspend()
        assert p._state == states.SUSPENDED
        assert p._state_history == [states.SCHEDULED, states.SUSPENDED]

        p = Pipeline()
        p._uid = 'pipeline.0000'
        p._state = states.SUSPENDED
        with pytest.raises(EnTKError):
            p.suspend()

# ------------------------------------------------------------------------------
    #
    @mock.patch.object(Pipeline, '__init__', return_value=None)
    def test_pipeline_resume(self, mocked_init):

        p = Pipeline()
        p._state = states.SUSPENDED
        p._state_history = [states.SCHEDULED, states.SUSPENDED]
        p.resume()
        assert p._state == states.SCHEDULED
        assert p._state_history == [states.SCHEDULED, states.SUSPENDED,
                                    states.SCHEDULED]

        p = Pipeline()
        p._uid = 'pipeline.0000'
        p._state = states.SCHEDULED
        p._state_history = [states.SCHEDULED]
        with pytest.raises(EnTKError):
            p.resume()
