import radical.pilot as rp

session = rp.Session()

c= rp.Context('ssh')
c.user_id = 'vivek91'
session.add_context(c)

pmgr = rp.PilotManager(session=session)

pdesc = rp.ComputePilotDescription()
pdesc.resource = 'xsede.stampede'
pdesc.runtime  = 10
pdesc.cores    = 1
pdesc.project = 'TG-MCB090174'

pilot = pmgr.submit_pilots(pdesc)

print dir(pilot)

print pilot.resource
print pilot.sandbox
