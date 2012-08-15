package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import java.io.Serializable;
import java.util.List;

/**
 *
 * @author jans
 */
public class MatchBean implements Serializable {

    private static final long serialVersionUID = -2737940579645137161L;
    private final String _tag;
    private final List<String> _pools;

    public MatchBean(List<String> list, String tag) {
        _pools = list;
        _tag = tag;
    }

    public String getTag() {
        return _tag;
    }

    public List<String> getPoolList() {
        return _pools;
    }
}
