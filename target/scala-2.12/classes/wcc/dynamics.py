'''Giulio Rossetti "RDyn: graph benchmark handling community
dynamics." Journal of Complex Networks 2017.
doi:10.1093/comnet/cnx016'''

from rdyn import RDyn
rdb = RDyn(size=1000, iterations=1000)
rdb.execute(simplified=True)

