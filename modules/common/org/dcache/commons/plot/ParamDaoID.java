package org.dcache.commons.plot;

/**
 *
 * @author timur and tao
 */
public class ParamDaoID implements PlotParameter{
    private String daoID;

    public ParamDaoID(String id) {
        this.daoID = id;
    }

    public String getDaoID() {
        return daoID;
    }

}
