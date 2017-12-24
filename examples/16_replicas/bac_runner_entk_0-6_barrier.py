from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
import os
import traceback
# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'


def generate_pipeline(num_tasks):

    # Create a Pipeline object
    p = Pipeline()

    #---------------------------------------------------------------------------
    # Stage 1

    s1 = Stage()

    # List of references to tasks in stage 1
    stage_1_ref = list()

    # Add tasks to stage 1
    for replica_ind in range(num_tasks):
        t1 = Task()
        t1.name = 'untar'
        t1.executable = ["/bin/bash"]
        t1.arguments = ['-l', '-c', 'tar zxvf {input1} -C ./'.format(input1 = rootdir+".tgz")]
        t1.cores = 1
        t1.copy_input_data  = ["$SHARED/"+rootdir+".tgz > "+rootdir+".tgz"]

        stage_1_ref.append("$Pipeline_{0}_Stage_{1}_Task_{2}/".format(p.uid, s1.uid, t1.uid))

        s1.add_tasks(t1)
    
    p.add_stages(s1)
    #---------------------------------------------------------------------------

    #---------------------------------------------------------------------------
    # Stage 2
    
    s2 = Stage()

    # List of references to tasks in stage 2
    stage_2_ref = list()

    # Add tasks to stage 2
    for replica_ind in range(num_tasks):
        t2 = Task()
        t2.name = 'preprep'
        t2.executable = ["/bin/bash"]
        t2.arguments = ['-l', '-c', 
                        'find -L {input1} -type f -print0 | xargs -0 sed -i \'s/REPX/{input2}/g\' ; mkdir -p {input1}/replicas/rep{input2}/equilibration; touch {input1}/replicas/rep{input2}/equilibration/holder; mkdir -p {input1}/replicas/rep{input2}/simulation; touch {input1}/replicas/rep{input2}/simulation/holder'.format(input1 = rootdir , input2 = replica_ind)]
        t2.cores = 1
    
        t2.copy_input_data = []
    
        for f in my_list:
            t2.copy_input_data.append("{stage1}/".format(stage1=stage_1_ref[replica_ind])+f+" > "+f)

        stage_2_ref.append("$Pipeline_{0}_Stage_{1}_Task_{2}/".format(p.uid, s2.uid, t2.uid))
        s2.add_tasks(t2)

    p.add_stages(s2)
    #---------------------------------------------------------------------------
    
    #---------------------------------------------------------------------------
    # Stage 3
    s3 = Stage()

    # List of references to tasks in stage 3
    stage_3_ref = list()

    # Add tasks to stage 3
    for replica_ind in range(num_tasks):
        t3 = Task()
        t3.name = 'stage3_namd'
        t3.executable = ['/u/sciteam/jphillip/NAMD_build.latest/NAMD_2.12_CRAY-XE-MPI-BlueWaters/namd2']
        t3.arguments = ["%s/mineq_confs/eq0.conf" % rootdir]
        t3.cores = coresp
        t3.mpi = True

        t3.copy_input_data = ['{stage2}/{input1}/replicas/rep{input2}/equilibration/holder > {input1}/replicas/rep{input2}/equilibration/holder'.format(stage2=stage_2_ref[replica_ind],input1 = rootdir, input2=replica_ind)]

        for f in my_list:
            t3.copy_input_data.append("{stage2}/".format(stage2=stage_2_ref[replica_ind])+f+" > "+f)
    
        stage_3_ref.append("$Pipeline_{0}_Stage_{1}_Task_{2}/".format(p.uid, s3.uid, t3.uid))
        s3.add_tasks(t3)

    p.add_stages(s3)
    #---------------------------------------------------------------------------

    #---------------------------------------------------------------------------
    # Stage 4
    
    s4 = Stage()

    # List of references to tasks in stage 4
    stage_4_ref = list()

    # Add tasks to stage 4
    for replica_ind in range(num_tasks):
        t4 = Task()
        t4.name = 'stage4_namd'
        t4.executable = ['/u/sciteam/jphillip/NAMD_build.latest/NAMD_2.12_CRAY-XE-MPI-BlueWaters/namd2']
        t4.arguments = ["%s/mineq_confs/eq1.conf" % rootdir]
        t4.cores = coresp
        t4.mpi = True

        t4.copy_input_data = [
            '{stage3}/{input1}/replicas/rep{input2}/equilibration/eq0.coor > {input1}/replicas/rep{input2}/equilibration/eq0.coor'.format(stage3=stage_3_ref[replica_ind],input1 = rootdir, input2 = replica_ind), 
            '{stage3}/{input1}/replicas/rep{input2}/equilibration/eq0.xsc > {input1}/replicas/rep{input2}/equilibration/eq0.xsc'.format(stage3=stage_3_ref[replica_ind], input1 = rootdir, input2 = replica_ind), 
            '{stage3}/{input1}/replicas/rep{input2}/equilibration/eq0.vel > {input1}/replicas/rep{input2}/equilibration/eq0.vel'.format(stage3=stage_3_ref[replica_ind], input1 = rootdir, input2 = replica_ind)]

        for f in my_list:
            t4.copy_input_data.append("{stage3}/".format(stage3=stage_3_ref[replica_ind])+f+" > "+f)

        stage_4_ref.append("$Pipeline_{0}_Stage_{1}_Task_{2}/".format(p.uid, s4.uid, t4.uid))
        s4.add_tasks(t4)

    p.add_stages(s4)
    #---------------------------------------------------------------------------

    #---------------------------------------------------------------------------
    # Stage 5
    
    s5 = Stage()

    # List of references to tasks in stage 5
    stage_5_ref = list()

    # Add tasks to stage 5
    for replica_ind in range(num_tasks):
        t5 = Task()
        t5.name = 'stage5_namd'
        t5.executable = ['/u/sciteam/jphillip/NAMD_build.latest/NAMD_2.12_CRAY-XE-MPI-BlueWaters/namd2']
        t5.arguments = ["%s/mineq_confs/eq2.conf" % rootdir]
        t5.cores = coresp
        t5.mpi = True

        t5.copy_input_data = [
            '{stage4}/{input1}/replicas/rep{input2}/equilibration/eq0.coor > {input1}/replicas/rep{input2}/equilibration/eq0.coor'.format(stage4=stage_4_ref[replica_ind],input1 = rootdir, input2 = replica_ind), 
            '{stage4}/{input1}/replicas/rep{input2}/equilibration/eq0.xsc > {input1}/replicas/rep{input2}/equilibration/eq0.xsc'.format(stage4=stage_4_ref[replica_ind],input1 = rootdir, input2 = replica_ind), 
            '{stage4}/{input1}/replicas/rep{input2}/equilibration/eq0.vel > {input1}/replicas/rep{input2}/equilibration/eq0.vel'.format(stage4=stage_4_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage4}/{input1}/replicas/rep{input2}/equilibration/eq1.xsc > {input1}/replicas/rep{input2}/equilibration/eq1.xsc'.format(stage4=stage_4_ref[replica_ind], input1 = rootdir, input2 = replica_ind),
            '{stage4}/{input1}/replicas/rep{input2}/equilibration/eq1.vel > {input1}/replicas/rep{input2}/equilibration/eq1.vel'.format(stage4=stage_4_ref[replica_ind], input1 = rootdir, input2 = replica_ind),
            '{stage4}/{input1}/replicas/rep{input2}/equilibration/eq1.coor > {input1}/replicas/rep{input2}/equilibration/eq1.coor'.format(stage4=stage_4_ref[replica_ind], input1 = rootdir, input2 = replica_ind)]


        for f in my_list:
            t5.copy_input_data.append("{stage4}/".format(stage4=stage_4_ref[replica_ind])+f+" > "+f)

        stage_5_ref.append("$Pipeline_{0}_Stage_{1}_Task_{2}/".format(p.uid, s5.uid, t5.uid))
        s5.add_tasks(t5)

    p.add_stages(s5)
    #---------------------------------------------------------------------------
    
    #---------------------------------------------------------------------------
    # Stage 6
    
    s6 = Stage()

    # List of references to tasks in stage 6
    stage_6_ref = list()

    # Add tasks to stage 6
    for replica_ind in range(num_tasks):
        t6 = Task()
        t6.name = 'stage6_namd'
        t6.executable = ['/u/sciteam/jphillip/NAMD_build.latest/NAMD_2.12_CRAY-XE-MPI-BlueWaters/namd2']
        t6.arguments = ["%s/sim_confs/sim1.conf" % rootdir]
        t6.cores = coresp
        t6.mpi = True

        t6.copy_input_data = [
            '{stage2}/{input1}/replicas/rep{input2}/simulation/holder > {input1}/replicas/rep{input2}/simulation/holder'.format(stage2=stage_2_ref[replica_ind],input1 = rootdir, input2=replica_ind), 
            '{stage5}/{input1}/replicas/rep{input2}/equilibration/eq0.coor > {input1}/replicas/rep{input2}/equilibration/eq0.coor'.format(stage5=stage_5_ref[replica_ind],input1 = rootdir, input2 = replica_ind), 
            '{stage5}/{input1}/replicas/rep{input2}/equilibration/eq0.xsc > {input1}/replicas/rep{input2}/equilibration/eq0.xsc'.format(stage5=stage_5_ref[replica_ind],input1 = rootdir, input2 = replica_ind), 
            '{stage5}/{input1}/replicas/rep{input2}/equilibration/eq0.vel > {input1}/replicas/rep{input2}/equilibration/eq0.vel'.format(stage5=stage_5_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage5}/{input1}/replicas/rep{input2}/equilibration/eq1.xsc > {input1}/replicas/rep{input2}/equilibration/eq1.xsc'.format(stage5=stage_5_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage5}/{input1}/replicas/rep{input2}/equilibration/eq1.vel > {input1}/replicas/rep{input2}/equilibration/eq1.vel'.format(stage5=stage_5_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage5}/{input1}/replicas/rep{input2}/equilibration/eq1.coor > {input1}/replicas/rep{input2}/equilibration/eq1.coor'.format(stage5=stage_5_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage5}/{input1}/replicas/rep{input2}/equilibration/eq2.xsc > {input1}/replicas/rep{input2}/equilibration/eq2.xsc'.format(stage5=stage_5_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage5}/{input1}/replicas/rep{input2}/equilibration/eq2.vel > {input1}/replicas/rep{input2}/equilibration/eq2.vel'.format(stage5=stage_5_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage5}/{input1}/replicas/rep{input2}/equilibration/eq2.coor > {input1}/replicas/rep{input2}/equilibration/eq2.coor'.format(stage5=stage_5_ref[replica_ind],input1 = rootdir, input2 = replica_ind)]

        for f in my_list:
            t6.copy_input_data.append("{stage5}/".format(stage5=stage_5_ref[replica_ind])+f+" > "+f)

        stage_6_ref.append("$Pipeline_{0}_Stage_{1}_Task_{2}/".format(p.uid, s6.uid, t6.uid))
        s6.add_tasks(t6)

    p.add_stages(s6)
    #---------------------------------------------------------------------------

    #---------------------------------------------------------------------------
    # Stage 7

    s7 = Stage()

    # List of references to tasks in stage 7
    stage_7_ref = list()

    # Add tasks to stage 7
    for replica_ind in range(num_tasks):
        t7 = Task()
        t7.name = 'stage7_tar'
        t7.executable = ["/bin/bash"]
        t7.arguments = ['-l', '-c', 'tar -hczf {input1}.tgz -C {input2}/replicas .'.format(input1 = 'rep%s'%replica_ind, input2 = rootdir)]
        t7.cores = 1
        t7.copy_input_data = [
            '{stage6}/{input1}/replicas/rep{input2}/equilibration/eq0.coor > {input1}/replicas/rep{input2}/equilibration/eq0.coor'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind), 
            '{stage6}/{input1}/replicas/rep{input2}/equilibration/eq0.xsc > {input1}/replicas/rep{input2}/equilibration/eq0.xsc'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind), 
            '{stage6}/{input1}/replicas/rep{input2}/equilibration/eq0.vel > {input1}/replicas/rep{input2}/equilibration/eq0.vel'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage6}/{input1}/replicas/rep{input2}/equilibration/eq1.xsc > {input1}/replicas/rep{input2}/equilibration/eq1.xsc'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage6}/{input1}/replicas/rep{input2}/equilibration/eq1.vel > {input1}/replicas/rep{input2}/equilibration/eq1.vel'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage6}/{input1}/replicas/rep{input2}/equilibration/eq1.coor > {input1}/replicas/rep{input2}/equilibration/eq1.coor'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage6}/{input1}/replicas/rep{input2}/equilibration/eq2.xsc > {input1}/replicas/rep{input2}/equilibration/eq1.xsc'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage6}/{input1}/replicas/rep{input2}/equilibration/eq2.vel > {input1}/replicas/rep{input2}/equilibration/eq1.vel'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage6}/{input1}/replicas/rep{input2}/equilibration/eq2.coor > {input1}/replicas/rep{input2}/equilibration/eq1.coor'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage6}/{input1}/replicas/rep{input2}/simulation/sim1.xsc > {input1}/replicas/rep{input2}/simulation/sim1.xsc'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage6}/{input1}/replicas/rep{input2}/simulation/sim1.vel > {input1}/replicas/rep{input2}/simulation/sim1.vel'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind),
            '{stage6}/{input1}/replicas/rep{input2}/simulation/sim1.coor > {input1}/replicas/rep{input2}/simulation/sim1.coor'.format(stage6=stage_6_ref[replica_ind],input1 = rootdir, input2 = replica_ind)]

        t7.download_output_data = ["rep{0}.tgz".format(replica_ind)]
        s7.add_tasks(t7)      

    p.add_stages(s7)
    
    #---------------------------------------------------------------------------

    return p


if __name__ == '__main__':


    try:
        coresp = 32
        rootdir = '2j6m-a698g'
        my_list = []
        
        for subdir, dirs, files in os.walk(rootdir):
            for file in files:
                #print os.path.join(subdir, file)
                my_list.append(os.path.join(subdir, file))

        pipelines = set()
        num_tasks=16
    
        pipelines.add(generate_pipeline(num_tasks))


        # Create a dictionary describe four mandatory keys:
        # resource, walltime, cores and project
        # resource is 'local.localhost' to execute locally
        res_dict = {
            'resource': 'ncsa.bw_aprun',
            'walltime': 1440,
            'cores': num_tasks * coresp,
            'project': 'bamm',
            'queue': 'high',
            'access_schema': 'gsissh'}

        # Create Resource Manager object with the above resource description
        rman = ResourceManager(res_dict)
        rman.shared_data = [rootdir + '.tgz']

        # Create Application Manager
        appman = AppManager(port=32775)

        # Assign resource manager to the Application Manager
        appman.resource_manager = rman

        # Assign the workflow as a set of Pipelines to the Application Manager
        appman.assign_workflow(pipelines)

        # Run the Application Manager
        appman.run()

    except Exception as ex: 
        print('Error: ',ex)
        print traceback.format_exc()
