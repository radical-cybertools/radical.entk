from radical.entk import Pipeline, Stage, Task, AppManager

def create_single_task():

    t1 = Task()
    t1.name = 'dummy_task'
    t1.executable = ['placeholder']
    t1.arguments = ['a','b','c']
    t1.copy_input_data = []
    t1.copy_output_data = []

    return t1

if __name__ == '__main__':

    p = Pipeline()
    p.name = 'seisflow'

    # Meshfem stage
    s1 = Stage()
    s1.name = 'meshfem'
    s1.tasks = create_single_task()
    p.add_stages(s1)


    # Specfem stages
    s2 = Stage()
    s2.name = 'specfem'
    for i in range(4):
        s2.add_tasks(create_single_task())
    p.add_stages(s2)


    # pypaw-process
    s3 = Stage()
    s3.name = 'pypaw-process'
    for i in range(4):
        s3.add_tasks(create_single_task())
    p.add_stages(s3)


    # pypaw-window-selection
    s4 = Stage()
    s4.name = 'pypaw-window-selection'
    for i in range(4):
        s4.add_tasks(create_single_task())
    p.add_stages(s4)


    # pypaw-measure-adjoint
    s5 = Stage()
    s5.name = 'pypaw-measure-adjoint'
    for i in range(4):
        s5.add_tasks(create_single_task())
    p.add_stages(s5)


    # pypaw-filter-windows
    s6 = Stage()
    s6.name = 'pypaw-filter-window'
    for i in range(4):
        s6.add_tasks(create_single_task())
    p.add_stages(s6)


    # pypaw-window-weights
    s7 = Stage()
    s7.name = 'pypaw-window-weights'
    for i in range(4):
        s7.add_tasks(create_single_task())
    p.add_stages(s7)


    # pypaw-adjoint-asdf
    s8 = Stage()
    s8.name = 'pypaw-adjoint-asdf'
    for i in range(4):
        s8.add_tasks(create_single_task())
    p.add_stages(s8)


    # pypaw-sum-adjoint
    s9 = Stage()
    s9.name = 'pypaw-sum-adjoint'
    s9.add_tasks(create_single_task())
    p.add_stages(s9)

    appman = AppManager()
    appman.assign_workflow(set([p]))
    appman.run()
