package diskCacheV111.poolManager;

import java.util.Comparator;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;

class LinkComparator implements Comparator<Link> {
    private final DirectionType _type;

    LinkComparator(DirectionType type) {
        _type = type;
    }

    @Override
    public int compare(Link link1, Link link2) {
        switch (_type) {
            case READ:
                // read
                return link1.getReadPref() == link2.getReadPref() ? link1.getName()
                        .compareTo(link2.getName()) : link1
                        .getReadPref() > link2.getReadPref() ? -1 : 1;
            case CACHE:
                // cache
                return link1.getCachePref() == link2.getCachePref() ? link1.getName()
                        .compareTo(link2.getName()) : link1
                        .getCachePref() > link2.getCachePref() ? -1 : 1;
            case WRITE:
                // write
                return link1.getWritePref() == link2.getWritePref() ? link1.getName()
                        .compareTo(link2.getName()) : link1
                        .getWritePref() > link2.getWritePref() ? -1 : 1;
            case P2P:
                // p2p
                int pref1 = link1.getP2pPref() < 0 ? link1.getReadPref() : link1.getP2pPref();
                int pref2 = link2.getP2pPref() < 0 ? link2.getReadPref() : link2.getP2pPref();
                return pref1 == pref2 ? link1.getName().compareTo(link2.getName()) : pref1 > pref2 ? -1 : 1;
        }
        throw new IllegalArgumentException("Wrong comparator mode");
    }

}
