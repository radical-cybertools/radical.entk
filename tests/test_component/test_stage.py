# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter, import-error

from unittest import TestCase
from random import shuffle

from   hypothesis import given, settings
import hypothesis.strategies as st
# import string

from radical.entk import Stage, Task
from radical.entk import states
from radical.entk.exceptions import TypeError, ValueError, MissingError

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

    @mock.patch('radical.utils.generate_id', return_value='stage.0000')
    def test_stage_initialization(self, mocked_generate_id):
        """
        ***Purpose***: Test if all attributes have, thus expect, the
        correct data types
        """

        s = Stage()

        self.assertEqual(s.uid, 'stage.0000')
        self.assertEqual(s.tasks, set())
        self.assertEqual(s.state, states.INITIAL)
        self.assertEqual(s.state_history, [states.INITIAL])
        self.assertEqual(s._task_count, 0)
        self.assertIsNone(s.name)
        self.assertIsNone(s.parent_pipeline['uid'])
        self.assertIsNone(s.parent_pipeline['name'])
        self.assertIsNone(s.post_exec)


    # ------------------------------------------------------------------------------
    #
    # @given(t=st.text(alphabet=string.ascii_letters +
    #                           string.punctuation.replace('.', ''),
    #                  min_size=10).filter(
    #     lambda x: any(symbol in x for symbol in string.punctuation)),
    @mock.patch('radical.utils.generate_id', return_value='stage.0000')
    @given(l=st.lists(st.text()),
           i=st.integers().filter(lambda x: type(x) == int),
           b=st.booleans(),
           se=st.sets(st.text()))
    def test_stage_exceptions(self, mocked_generate_id, l, i, b, se):
        """
        ***Purpose***: Test if correct exceptions are raised when attributes are
        assigned unacceptable values.
        """

        s = Stage()

        data_type = [l, i, b, se]

        for data in data_type:

            if not isinstance(data, str):
                with self.assertRaises(TypeError):
                    s.name = data

            # if isinstance(data,str):
            #     with self.assertRaises(ValueError):
            #         s.name = data

            with self.assertRaises(TypeError):
                s.tasks = data

            with self.assertRaises(TypeError):
                s.add_tasks(data)

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    @given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
    def test_stage_validate_entities(self, mocked_init, t, l, i, b, se):

        s = Stage()

        data_type = [t, l, i, b, se]

        for data in data_type:
            with self.assertRaises(TypeError):
                s._validate_entities(data)

        t = mock.MagicMock(spec=Task())
        self.assertIsInstance(s._validate_entities(t), set)

        t1 = mock.MagicMock(spec=Task())
        t2 = mock.MagicMock(spec=Task())
        self.assertEqual(set([t1, t2]), s._validate_entities([t1, t2]))


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    def test_stage_task_assignment(self, mocked_init):
        """
        ***Purpose***: Test if necessary attributes are automatically updates upon task assignment
        """

        global_tasks = set()

        # ------------------------------------------------------------------------------
        #
        def _validate_entities_side_effect(things):
            nonlocal global_tasks
            global_tasks.add(things)
            return global_tasks

        s = Stage()
        s._validate_entities = mock.MagicMock(side_effect=_validate_entities_side_effect)
        t = mock.MagicMock(spec=Task())
        s.tasks = t

        self.assertIsInstance(s.tasks, set)
        self.assertEqual(s._task_count, 1)
        self.assertIn(t, s.tasks)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    @given(l=st.lists(st.text()),
        i=st.integers().filter(lambda x: type(x) == int),
        b=st.booleans())
    def test_stage_parent_pipeline_assignment(self, mocked_init, l, i, b):

        s = Stage()
        data_type = [l, i, b]
        for data in data_type:
            with self.assertRaises(TypeError):
                s.parent_pipeline = data

        s = Stage()
        data = {'test': 'pipeline.0000'}
        s.parent_pipeline = data
        self.assertEqual(s._p_pipeline, {'test': 'pipeline.0000'})

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    @given(t=st.text(),
        l=st.lists(st.text()),
        i=st.integers().filter(lambda x: type(x) == int),
        b=st.booleans())
    def test_stage_state_assignment(self, mocked_init, t, l, i, b):

        s = Stage()
        s._uid = 'test_stage'

        data_type = [l, i, b]

        for data in data_type:
            with self.assertRaises(TypeError):
                s.state = data

        if isinstance(t, str):
            with self.assertRaises(ValueError):
                s.state = t

        s = Stage()
        s._uid = 'test_stage'
        s._state = None
        s._state_history = list()
        state_history = list()
        states_list = list(states._stage_state_values.keys())
        shuffle(states_list)
        for val in states_list:
            s.state = val
            if val != states.SUSPENDED:
                state_history.append(val)
            self.assertEqual(s._state, val)
            self.assertEqual(s._state_history, state_history)

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    @given(l=st.lists(st.text()),
        d=st.dictionaries(st.text(), st.text()))
    def test_stage_post_exec_assignment(self, mocked_init, l, d):

        s = Stage()
        s._uid = 'test_stage'

        def func():
            return True

        with self.assertRaises(TypeError):
            s.post_exec = l

        with self.assertRaises(TypeError):
            s.post_exec = d


        s.post_exec = func
        self.assertEqual(s._post_exec, func)

        class Tmp(object):

            def func(self):
                return True

        tmp = Tmp()
        s.post_exec = tmp.func
        self.assertEqual(s._post_exec, tmp.func)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    def test_stage_task_addition(self, mocked_init):

        s = Stage()
        s._p_pipeline = {'uid': None, 'name': None}
        s._uid = 'stage.0000'
        s._name = None
        s._tasks = set()
        t1 = mock.MagicMock(spec=Task())
        t2 = mock.MagicMock(spec=Task())
        s.add_tasks(set([t1, t2]))

        self.assertIsInstance(s.tasks, set)
        self.assertEqual(s._task_count, 2)
        self.assertIn(t1, s.tasks)
        self.assertIn(t2, s.tasks)

        s = Stage()
        s._uid = 'stage.0000'
        s._name = None
        s._p_pipeline = {'uid': None, 'name': None}
        s._tasks = set()
        t1 = mock.MagicMock(spec=Task())
        t2 = mock.MagicMock(spec=Task())
        s.add_tasks([t1, t2])

        self.assertIsInstance(s.tasks, set)
        self.assertEqual(s._task_count, 2)
        self.assertIn(t1, s.tasks)
        self.assertIn(t2, s.tasks)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    def test_stage_to_dict(self, mocked_init):

        s = Stage()        
        s._uid = 'stage.0000'
        s._name = 'test_stage'
        s._state = states.INITIAL
        s._state_history = [states.INITIAL]
        s._p_pipeline = {'uid': 'pipeline.0000', 'name': 'parent'}

        self.assertEqual(s.to_dict(),{'uid': 'stage.0000',
                                      'name': 'test_stage',
                                      'state': states.INITIAL,
                                      'state_history': [states.INITIAL],
                                      'parent_pipeline': {'uid': 'pipeline.0000', 
                                                          'name': 'parent'}})


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    def test_stage_from_dict(self, mocked_init):

        d = {'uid': 're.Stage.0000',
            'name': 's1',
            'state': states.DONE,
            'state_history': [states.INITIAL, states.DONE],
            'parent_pipeline': {'uid': 'p1',
                                'name': 'pipe1'}
            }

        s = Stage()
        s._uid = None
        s._name = None
        s._state = None
        s._state_history = None
        s._p_pipeline = None
        s.from_dict(d)

        self.assertEqual(s._uid, d['uid'])
        self.assertEqual(s._name, d['name'])
        self.assertEqual(s._state, d['state'])
        self.assertEqual(s._state_history, d['state_history'])
        self.assertEqual(s._p_pipeline, d['parent_pipeline'])


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    def test_stage_set_tasks_state(self, mocked_init):

        s = Stage()
        s._uid = 'stage.0000'
        t1 = mock.MagicMock(spec=Task())
        t2 = mock.MagicMock(spec=Task())
        s._tasks = set([t1, t2])

        with self.assertRaises(ValueError):
            s._set_tasks_state(2)

        s._set_tasks_state(states.DONE)
        self.assertEqual(t1.state, states.DONE)
        self.assertEqual(t2.state, states.DONE)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    def test_stage_check_complete(self, mocked_init):

        s = Stage()
        s._uid = 'stage.0000'
        t1 = mock.MagicMock(spec=Task())
        t2 = mock.MagicMock(spec=Task())
        s._tasks = set([t1, t2])

        self.assertFalse(s._check_stage_complete())
        for t in s._tasks:
            t.state = states.DONE
        self.assertTrue(s._check_stage_complete())


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    def test_stage_validate(self, mocked_init):

        s = Stage()
        s._uid = 'stage.0000'
        s._state = 'test'
        with self.assertRaises(ValueError):
            s._validate()

        s = Stage()
        s._uid = 'stage.0000'
        s._state = states.INITIAL
        s._tasks = None
        with self.assertRaises(MissingError):
            s._validate()

        s = Stage()
        s._uid = 'stage.0000'
        t = mock.MagicMock(spec=Stage)
        t._validate = mock.MagicMock(return_value=True)
        s._tasks = set([t])
        s._state = states.INITIAL
        s._validate()

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    def test_stage_assign_uid(self, mocked_init):

        s = Stage()
        s._uid = 'stage.0000'
        self.assertEqual(s.uid, 'stage.0000')


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Stage, '__init__', return_value=None)
    def test_luid(self, mocked_init):

        s = Stage()
        s._p_pipeline = {'uid': 'pipe.0000', 'name': None}
        s._uid = 'stage.0000'
        s._name = None

        self.assertEqual(s.luid, 'pipe.0000.stage.0000')

        s = Stage()
        s._p_pipeline = {'uid': 'pipe.0000', 'name': 'test_pipe'}
        s._uid = 'stage.0000'
        s._name = None

        self.assertEqual(s.luid, 'test_pipe.stage.0000')

        s = Stage()
        s._p_pipeline = {'uid': 'pipe.0000', 'name': None}
        s._uid = 'stage.0000'
        s._name = 'test_stage'

        self.assertEqual(s.luid, 'pipe.0000.test_stage')

        s = Stage()
        s._p_pipeline = {'uid': 'pipe.0000', 'name': 'test_pipe'}
        s._uid = 'stage.0000'
        s._name = 'test_stage'

        self.assertEqual(s.luid, 'test_pipe.test_stage')
