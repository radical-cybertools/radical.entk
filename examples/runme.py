from radical.entk import Pipeline, Stage, Task, AppManager

def create_single_task():

    t1 = Task()
    t1.name = 'simulation'
    t1.executable = ['gmx mdrun']
    t1.arguments = ['a','b','c']
    t1.copy_input_data = []
    t1.copy_output_data = []

    return t1

if __name__ == '__main__':

    p1 = Pipeline()
    p1.name = 'p1'
    p2 = Pipeline()
    p2.name = 'p2'

    stages=3

    for cnt in range(stages):
        s = Stage()
        s.name = 's_%s'%cnt
        s.tasks = create_single_task()
        s.add_tasks(create_single_task())

        p1.add_stages(s)

    for cnt in range(stages):
        s = Stage()
        s.name = 's-%s'%cnt
        s.tasks = create_single_task()
        s.add_tasks(create_single_task())

        p2.add_stages(s)

    appman = AppManager()
    appman.resubmit_failed = True
    appman.assign_workload(set([p1,p2]))
    appman.run()
