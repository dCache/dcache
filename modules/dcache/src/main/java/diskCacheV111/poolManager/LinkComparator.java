package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import java.util.Comparator;

class LinkComparator implements Comparator<Link> {
    private final DirectionType _type;

    LinkComparator(DirectionType type) {
        _type = type;
    }

    public int compare(Link link1, Link link2) {
        switch (_type) {
            case READ:
                // read
                return link1._readPref == link2._readPref ? link1._name.compareTo(link2._name) : link1._readPref > link2._readPref ? -1 : 1;
            case CACHE:
                // cache
                return link1._cachePref == link2._cachePref ? link1._name.compareTo(link2._name) : link1._cachePref > link2._cachePref ? -1 : 1;
            case WRITE:
                // write
                return link1._writePref == link2._writePref ? link1._name.compareTo(link2._name) : link1._writePref > link2._writePref ? -1 : 1;
            case P2P:
                // p2p
                int pref1 = link1._p2pPref < 0 ? link1._readPref : link1._p2pPref;
                int pref2 = link2._p2pPref < 0 ? link2._readPref : link2._p2pPref;
                return pref1 == pref2 ? link1._name.compareTo(link2._name) : pref1 > pref2 ? -1 : 1;
        }
        throw new IllegalArgumentException("Wrong comparator mode");
    }

}
