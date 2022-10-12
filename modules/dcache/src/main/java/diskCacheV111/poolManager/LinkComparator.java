package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import java.util.Comparator;
import java.util.function.ToIntFunction;

class LinkComparator implements Comparator<Link> {

    private final DirectionType _type;

    LinkComparator(DirectionType type) {
        _type = type;
    }

    @Override
    public int compare(Link link1, Link link2) {

        ToIntFunction<Link> toPref;

        switch (_type) {
            case READ:
                toPref = l -> l.getReadPref();
                break;
            case CACHE:
                toPref = l -> l.getCachePref();
                break;
            case WRITE:
                toPref = l -> l.getWritePref();
                break;
            case P2P:
                // Backward compatibility: if p2p preference is negative, then use read pref.
                toPref = l -> l.getP2pPref() < 0 ? l.getReadPref() : l.getP2pPref();
                break;
            default:
                throw new IllegalArgumentException("Wrong comparator mode");
        }

        int pref1 = toPref.applyAsInt(link1);
        int pref2 = toPref.applyAsInt(link2);

        return pref1 == pref2 ? link1.getName().compareTo(link2.getName())
              : pref1 > pref2 ? -1 : 1;
    }

}
