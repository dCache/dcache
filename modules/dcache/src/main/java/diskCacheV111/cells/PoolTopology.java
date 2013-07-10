package diskCacheV111.cells;


/**

   Pool Topology provides information about Canonical Distance to Pool
   and hierarchy level. The posible values for distance are LOCAL, NEAR,
   SITE and FAR. PoolTopology are static informatiom, which stored in
   PoolInfoDB of Pool Manager.

*/


public class PoolTopology {


	String _poolName;
	int _distance;
	int _hierachyLevel;

	public PoolTopology(String poolName, int distance, int hierachyLevel) {
		_poolName = poolName;
		_distance = distance;
		_hierachyLevel = hierachyLevel;
	}

    String getPoolName() {
		return _poolName;
	}

    // LOCAL, NEAR, SITE, FAR
	int getCanonicalDistance() {
		return _distance;
	}

    int getHierachyLevel() {
		return _hierachyLevel;
	}
}
