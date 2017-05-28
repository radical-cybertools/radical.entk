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

    pipes = []
    for i in range(4):
        p = Pipeline()

        s1 = Stage()
        s1.name = 'prediction'
        s1.tasks = create_single_task()
        p.add_stages(s1)

        s2 = Stage()
        s2.name = 'quality-check'
        s2.tasks = create_single_task()
        p.add_stages(s2)

        pipes.append(p)


    appman = AppManager()
    appman.assign_workflow(set(pipes))
    appman.run()
