def test_user_module_loads():

    """
    **Purpose**: Test if all the user components can be imported by the user
    """

    import radical.entk
    from radical.entk import Task
    from radical.entk import Stage
    from radical.entk import Pipeline
    from radical.entk import AppManager
    from radical.entk import states
    from radical.entk import ResourceManager

