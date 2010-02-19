package org.dcache.webadmin.controller.util;

import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.view.beans.PoolBean;

/**
 * Does the mapping between modelobjects and viewobjects
 * @author jans
 */
public class BeanDataMapper {

    public static PoolBean poolModelToView(Pool poolBusinessObject,
            NamedCell namedCellBusinessObject) {
        PoolBean returnPoolBean = poolModelToView(poolBusinessObject);
        returnPoolBean.setDomainName(namedCellBusinessObject.getDomainName());

        return returnPoolBean;
    }

    public static PoolBean poolModelToView(Pool poolBusinessObject) {
        PoolBean returnPoolBean = new PoolBean();
        returnPoolBean.setEnabled(poolBusinessObject.isEnabled());
        returnPoolBean.setFreeSpace(poolBusinessObject.getFreeSpace());
        returnPoolBean.setFreeSpace(poolBusinessObject.getFreeSpace());
        returnPoolBean.setName(poolBusinessObject.getName());
        returnPoolBean.setPreciousSpace(poolBusinessObject.getPreciousSpace());
        returnPoolBean.setTotalSpace(poolBusinessObject.getTotalSpace());
        returnPoolBean.setUsedSpace(poolBusinessObject.getUsedSpace());
        return returnPoolBean;
    }
}
