package diskCacheV111.cells;

/**
   The pool selection methods are called with a list of PoolQualities.
   Pool Qualities keeps informatio about costs and topology.
*/



public class PoolQuality {

	private String _poolName;
	private int _preference;
	private double _spaceCost;
	private boolean _hasFile;
	private PoolTopology _poolTopology;

	public PoolQuality( String poolName, int preference, double spaceCost,
					boolean hasFile, PoolTopology poolTopology) {
		_poolName = poolName;
		_preference = preference;
		_spaceCost = spaceCost;
		_poolTopology = poolTopology;
	}

    public String getName() {
		return _poolName;
	}

    public int getPreference() {
		return _preference;
	}

    public double getSpaceCost() {
		return _spaceCost;
	}

    public boolean hasFile() {
		return _hasFile;
	}

    public PoolTopology getTopology() {
		return _poolTopology;
	}
}
