from radical.entk import Pipeline, Stage, Task, AppManager

def create_single_task():

    t1 = Task(name='simulation')
    t1.environment = ['module load gromacs']
    t1.executable = ['gmx mdrun']
    t1.arguments = ['a','b','c']
    t1.copy_input_data = []
    t1.copy_output_data = []

    return t1

if __name__ == '__main__':

    p1 = Pipeline(name='p1')
    stages=3

    for cnt in range(stages):
        s = Stage(name='s%s'%cnt)
        s.tasks = create_single_task()
        s.add_tasks(create_single_task())

        p1.add_stages(s)

    appman = AppManager()
    appman.assign_workload(p1)
    appman.run()