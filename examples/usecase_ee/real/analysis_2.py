import pdb
import os
import numpy as np
import subprocess
import random
import glob
#import launchjob
import shutil
import argparse

def get_weights_from_mbar(free_energy_file):

    f = open(fre_energy_file,'r')
    lines = f.readlines()
    f.close()
    weights = []
    for l in lines:
        if l[:9] == '   States':
            vals = l.split()
            # find which column is MBAR.
            i = 1
            for v in vals:
                if v[i] == 'MBAR':
                    mbar_column = i
                    i+=2
        else:
            if '+-' in l and '--' in l:
                vals = l.split()
                weights.append(vals[mbar_column])
                
    return np.array(weights)

def free_energy_analysis(dir, filelist, template, new_mdp_name):

    # run alchemical analysis on the expanded ensemble dhdl's
    # identify the dhdl files, copy them to a new directory, renaming if necessary, and figure out how many of them we should use
    dhdlfiles = glob.glob(os.path.join(dir,'*dhdl.xvg'))

    #maximum number of generations
    max_all_gens = 1000
    generations = []
    runs = []
    for d in dhdlfiles:
        vals = d.split('_')
        run = int(vals[1].replace('run',''))
        if run not in runs:
            runs.append(run)
        gen = int(vals[2].replace('gen',''))
        if gen not in generations:
            generations.append(gen)

    # now, what is the maximum generation for each of the runs.
    maxgens_per_run = np.zeros(len(runs))
    for d in dhdlfiles:
        vals = d.split('_')
        run = int(vals[1].replace('run',''))
        gen = int(vals[2].replace('gen',''))
        if gen > maxgens_per_run[runs.index(run)]:
            maxgens_per_run[runs.index(run)] = gen

    # which files to analyze? For now, ignore the 1/3 of the oldest, rounded up
    # 1 generation - ignore none
    # 2 generations - ignore 1, 
    # 3 generations - ignore 1, 
    # 4 generations - ignore 2, 
    maxgen = np.max(generations)
    analysis_cutoff = int(np.ceil(maxgen/3.0))
    if (analysis_cutoff - maxgen) < 1:
        analysis_cutoff = 0

    # which generations are going to be analyzed?    
    analysis_gens = []    
    for g in range(1,maxgen+1):
        if g > analysis_cutoff:
            analysis_gens.append(g)
            
    # figure out how many analyses have been done, make a new directory
    analyses = glob.glob(os.path.join(dir,'analyze_*'))
    max_analyses = 0
    for a in analyses:
        # remove if empty
        if not os.listdir(a):
            rmdir(a)
            continue
        vals = a.split('_')
        nanalyses = int(vals[1])
        if nanalyses > max_analyses:
            max_analyses = nanalyses
    analysis_dir = 'analyze' + '_' + str(max_analyses+1)
    os.mkdir(os.path.join(dir, analysis_dir))

    # copy the dhdl files into a new directory, so we can just read all of them in the folder.
    for d in dhdlfiles:
        vals = d.split('_')
        run = int(vals[1].replace('run',''))
        gen = int(vals[2].replace('gen',''))
        if gen in analysis_gens:
            folder = vals[0].split('/')[0]
            newfile = vals[0].split('/')[1] + '_' + str(max_all_gens*run+gen) + '_dhdl.xvg'
            absolute_analysis_dir = os.path.join(folder, analysis_dir)
            shutil.copyfile(d, os.path.join(absolute_analysis_dir,newfile))

    try:
        subprocess.call(['python', 'alchemical_analysis.py', '--units', 'kBT', '-t', '300', '-d', absolute_analysis_dir, '-p', 'PLCpep7_', '-x', '-i', '1000'])

        # open the file and read the weights (in units of kT)
        free_energy_file = os.path.join(absolute_analysis_dir, 'results.txt')
        f = open(free_energy_file, 'r')
    except:

        # Use the backup from last iteration
        free_energy_file = os.path.join(os.path.basename(absolute_analysis_dir), 'results_bak.txt')

        print 'BACKUP USED'
    
        f = open(free_energy_file,'r')


    lines = f.readlines()
    # just use the MBAR total for the lines
    dweights = [0] # the first weight is always zero

    for l in lines:
        if 'States' in l:
            vals = l.split()
            #mbar_index = vals.index('MBAR')
            # little more complicated, but will always be doing MBAR for now, hard code for now.
            mbar_index = 18
        if ' -- ' in l:
            dweights.append(float(l.split()[mbar_index]))
    dweights = np.array(dweights)        
    weights = np.zeros(len(dweights))
    for i in range(0,len(weights)-1):
        weights[i+1] = weights[i] + dweights[i+1]

    # sort out the logfiles
    logs = []
    for f in filelist:
        if '.log' in f:
            logs.append(f)

    ilog = 0
    seedbase = random.randint(0, 100000000) # could replace by maxint (short integer)
    for l in logs:

        # we need to find the latest generation.
        vals = l.split('_')
        run = int(vals[1].replace('run',''))
        gen = int((vals[2].replace('gen','')).replace('.log',''))
        if gen != maxgens_per_run[runs.index(run)]:
            continue # we only want to run the last generation, so if it's not the last one, don't bother here.

        [name, suffix] = l.split('.')[1:]
        name=name[1:]
        final_lambda_state, coupled_frames = get_info_from_dhdl(name + '_dhdl.xvg', trajectory_frequency=10) 
        # trajectory_frequency=10 corresponds to what we ran; should extract this automatically from the mdp.
        # it's just nstxout-compressed/nstdhdl
        equilibrated, wldelta = get_info_from_logfile(l)
        seed = seedbase + ilog

        # extract the generation
        run_name = l.split('_')
        next_gen = int((run_name[2].split('.')[0]).replace('gen','')) + 1

        # new mdp name
        #new_mdp_name = run_name[0] + '_' + run_name[1] + '_' + 'gen' + str(next_gen) + '.mdp'

        # now we've extracted the information, create the start file for the next generation.
        generate_new_mdp(template, new_mdp_name, weights, wldelta, equilibrated, final_lambda_state, seed)
        #vals = run_name[0].split('/')
        #new_gen_name = (new_mdp_name.replace('.mdp','')).split('/')[1]

        # print out for later the states that are coupled for configurational analysis
        f = open(name + '.states','w')
        for s in coupled_frames:
            f.write(str(s) + '\n')
        f.close()
            
        # launch the new jobs
        #launchjob.runonegen(new_gen_name, vals[0], new_mdp_name, name + '.gro', vals[1] + '.top', vals[1] + '.ndx')


def get_info_from_logfile(logfile):

    # read logfile lines
    f = open(logfile,'r')
    lines = f.readlines()
    f.close()    

    wldelta = None
    is_equilibrated = True
    for l in lines:
        if 'Wang-Landau incrementor' in l:
            is_equilibrated = False 
            vals = l.split(':')
            wldelta = float(vals[1])

    # we don't break, because we need the LAST wldelta
        
    return is_equilibrated, wldelta
        
def get_info_from_dhdl(dhdl, trajectory_frequency=None, coupled_states = [0,1,2,3]):

    # we want the final state, and a list of the frames with trajectories that are coupled.

    # read dhdl lines
    f = open(dhdl,'r')
    lines = f.readlines()
    f.close()

    # first, check the final state:

    # what's the last line?
    lastline = lines[-1]
    values = lastline.split()
    final_state = int(float(values[1])) # figure out why it's sometimes a float . . . 

    # next, if we are looking at the trajectories
    coupled_frames = []
    if trajectory_frequency != None:
        i = 0
        ilegend = 1
        stateindex = 1
        for l in lines:
            if l[0]!= '#' and l[0]!= '@':
                i+=1
                if i%trajectory_frequency == 0:
                    vals = l.split()
                    state = int(float(vals[stateindex]))
                    if state in coupled_states:
                        coupled_frames.append(i)
        coupled_frames = np.array(coupled_frames)

    return final_state, coupled_frames

def generate_new_mdp(template, new_mdp_name, weights, wldelta, equilibrated, init_lambda_state, seed):

    '''
    inputs: 
       template (string): file name of the template that will be used to make the new generation
       new_mdp_name (string): the name of the new mpd.
       weights (array of floats): the new weights
       wldelta (float): the current wl delta
       equilibrated (bool): whether the weights are currently equilibrate
       init_lambda_state (int): the current lambda state
       seed (int): seed for this run

    outputs: 
       none

    '''

    # read template lines
    f = open(template,'r')
    lines = f.readlines()
    f.close()

    f = open(new_mdp_name,'w')
    # write new mdp lines
    for l in lines:
        if 'REPLACE' in l:
            l = l.replace('REPLACELAMBDA',str(init_lambda_state)) 
            l = l.replace('REPLACEINITWLDELTA',str(wldelta)) 
            if equilibrated:
                l = l.replace('REPLACELMCSTATS','no')
            else:
                l = l.replace('REPLACELMCSTATS','wang-landau')
            l = l.replace('REPLACEGENSEED',str(seed))
            weights_array = ["%.3f" % w for w in weights]
            weights_string = ' '.join(weights_array)
            l = l.replace('REPLACEINITLAMBDAWEIGHTS',weights_string)
        f.write(l)

def configuration_analysis(dir, filelist):
    ''' 
    inputs
    dir (string) - the directory the files to analyze are found in
    name (string) - the common prefix of the files of interest
    filelist (array of strings) - a list of the files to analyze
    '''

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--newname', help='new name for reweighted mdp file')
    parser.add_argument('--template', help='name of template mdp file')
    args = parser.parse_args()

    #dir = 'PLCpep7runs'
    dir = './'
    filelist = glob.glob(os.path.join(dir,'PLCpep7*'))
    #template = 'PLCpep7_template.mdp'
    free_energy_analysis(dir, filelist, args.template, args.newname)
