package org.dcache.tests.util;

import diskCacheV111.util.ThreadPool;
import dmg.cells.nucleus.CellAdapter;

public class CurrentThreadExceutorHelper implements ThreadPool {

    public CurrentThreadExceutorHelper(CellAdapter cell) {
    }


    public int getCurrentThreadCount() {
        return 1;
    }

    public int getMaxThreadCount() {
        return 1;
    }

    public int getWaitingThreadCount() {
        return 0;
    }

    public void invokeLater(Runnable runner, String name)
            throws IllegalArgumentException {
       runner.run();
    }

    public void setMaxThreadCount(int maxThreadCount)
            throws IllegalArgumentException {
        // go a way!
    }

    @Override
    public String toString() {
        return "CurrentThreadExceutorHelper";
    }
}