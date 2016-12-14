__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.pilot as rp
import os

ENSEMBLE_SIZE = 4

if __name__ == '__main__':

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways..
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {
                'resource'      : 'local.localhost',
                'runtime'       : 15,  # pilot runtime (min)
                'exit_on_error' : True,
                'project'       : None,
                'queue'         : None,
                'access_schema' : None,
                'cores'         : 4,
                }

        pdesc = rp.ComputePilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)


        # Create a list of RP directives - 1 for each shared file to be transferred
        shared_data = []
        data_src = [
                        './input_data/CMTSOLUTION',
                        './input_data/STATIONS',
                        './input_data/bin/specfem_mockup',
                        './input_data/Par_file',
                        './input_data/addressing.txt',
                        './input_data/values_from_mesher.h'
                    ]

        for file in data_src:


            abspath = os.path.dirname(os.path.abspath(__file__)) + file[1:]
            data_directive = {
                                'source': abspath,
                                'target': 'staging:///%s'%os.path.basename(file),
                                'action': rp.TRANSFER
                            }

            shared_data.append(data_directive)

        # Stage-in the shared data
        pilot.stage_in(shared_data)

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        cuds = []
        for i in range(ENSEMBLE_SIZE):

            # create a new CU description, and fill it.            
            cud = rp.ComputeUnitDescription()
            cud.pre_exec = [
                                'mkdir DATA',
                                    'mv Par_file DATA/',

                                'mkdir DATABASE_MPI',

                                'mkdir OUTPUT_FILES',
                                    'mv addressing.txt OUTPUT_FILES/',
                                    'mv values_from_mesher.h OUTPUT_FILES/',

                                'mkdir run0001',
                                    'mkdir run0001/DATA',
                                        'cp CMTSOLUTION STATIONS run0001/DATA/',
                                    'cd run0001/',
                                        'ln -s ../DATABASE_MPI DATABASE_MPI',
                                        'cd ..',
                                    'mkdir run0001/OUTPUT_FILES',

                                    'mkdir bin',
                                        'chmod +x specfem_mockup',
                                        'cp specfem_mockup bin/'

                            ]

            cud.executable = './bin/specfem_mockup'
            cud.mpi = True
            cud.cores = 2
            
            ip_data = []
            for file in data_src:

                data_directive = {
                                'source': 'staging:///%s'%os.path.basename(file),
                                'target': os.path.basename(file),
                                'action': rp.COPY
                            }

                ip_data.append(data_directive)

            cud.input_staging = ip_data

            cuds.append(cud)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
        umgr.wait_units()

    except Exception as ex:

        print 'Error: %s' %ex
        raise


    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print 'exit requested\n'

    finally:

        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        print 'finalize'
        session.close()
