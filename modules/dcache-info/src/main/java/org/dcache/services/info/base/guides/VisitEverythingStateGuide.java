package org.dcache.services.info.base.guides;

import org.dcache.services.info.base.StateGuide;
import org.dcache.services.info.base.StatePath;

public class VisitEverythingStateGuide implements StateGuide {

    @Override
    public boolean isVisitable(StatePath path) {
        return true;
    }
}
