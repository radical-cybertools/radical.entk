import argparse
import numpy as np

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



if __name__ == '__main__':

    try:

        parser = argparse.ArgumentParser()

        parser.add_argument('--template', help='name of template mdp file')
        parser.add_argument('--newname', help='new name for reweighted mdp file')
        parser.add_argument('--wldelta', help='wldelta')
        parser.add_argument('--equilibrated', help='equil')
        parser.add_argument('--lambda_state', help='initial lambda state')
        parser.add_argument('--seed', help='seed value')

        args = parser.parse_args()

        if args.equilibrated == 'False':
            equil = False
        else:
            equil = True

        f = open(args.template,'r')
        lines = f.readlines()
        for l in lines:
            if 'fep-lambdas' in l:
                nlambda = len(l.split()[2:])

        weights = np.zeros(nlambda)

        generate_new_mdp(args.template, args.newname, weights, int(args.wldelta), equil, int(args.lambda_state), int(args.seed))

    except Exception, ex:

        print 'Error: {0}'.format(ex)
        raise