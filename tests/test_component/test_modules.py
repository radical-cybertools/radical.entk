
from unittest import TestCase

# pylint: disable=protected-access, unused-argument, unused-import
# pylint: disable=no-value-for-parameter


# ------------------------------------------------------------------------------
#
class TestBase(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_user_module_loads(self):

        """
        **Purpose**: Test if all the user components can be imported by the user
        """

        import radical.entk                                         # noqa: F401
        from radical.entk import Task                               # noqa: F401
        from radical.entk import Stage                              # noqa: F401
        from radical.entk import Pipeline                           # noqa: F401
        from radical.entk import AppManager                         # noqa: F401
        from radical.entk import states                             # noqa: F401


# ------------------------------------------------------------------------------

