// $Id: PoolPreferenceLevel.java,v 1.2 2007-05-24 13:51:11 tigran Exp $

package diskCacheV111.poolManager;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/*
 * @Immutable
 */
public class PoolPreferenceLevel implements Serializable {

    private static final long serialVersionUID = 8671595392621995474L;

    private final String _tag;
    private final List<String> _list;

    PoolPreferenceLevel(List<String> list, String tag) {
        _list = list;
        _tag = tag;
    }

    public String getTag() {
        return _tag;
    }

    public List<String> getPoolList() {
        return _list;
    }

    public static List<String>[] fromPoolPreferenceLevelToList(PoolPreferenceLevel[] level) {
        List<String>[] prioPools = new ArrayList[level.length];
        for (int i = 0; i < level.length; i++) {
            prioPools[i] = level[i].getPoolList();
        }

        return prioPools;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
              .add("tag", _tag)
              .add("pools", _list)
              .omitNullValues()
              .toString();
    }
}
