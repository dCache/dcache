package org.dcache.webadmin.controller.util;

import org.dcache.webadmin.model.businessobjects.MoverQueue;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.view.beans.PoolBean;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.beans.PoolRequestQueue;

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

    public static PoolQueueBean poolQueueModelToView(Pool poolBusinessObject,
            NamedCell namedCellBusinessObject) {
        PoolQueueBean returnPoolQueueBean = poolQueueModelToView(poolBusinessObject);
        returnPoolQueueBean.setDomainName(namedCellBusinessObject.getDomainName());

        return returnPoolQueueBean;
    }

    public static PoolQueueBean poolQueueModelToView(Pool poolBusinessObject) {
        PoolQueueBean returnPoolQueueBean = new PoolQueueBean();
        returnPoolQueueBean.setName(poolBusinessObject.getName());
        returnPoolQueueBean.setMovers(queueModelToView(poolBusinessObject.getMovers()));
        returnPoolQueueBean.setP2p(queueModelToView(poolBusinessObject.getP2p()));
        returnPoolQueueBean.setP2pclient(queueModelToView(poolBusinessObject.getP2pclient()));
        returnPoolQueueBean.setP2pserver(queueModelToView(poolBusinessObject.getP2pserver()));
        returnPoolQueueBean.setRegular(queueModelToView(poolBusinessObject.getRegular()));
        returnPoolQueueBean.setRestores(queueModelToView(poolBusinessObject.getRestores()));
        returnPoolQueueBean.setStores(queueModelToView(poolBusinessObject.getStores()));
        return returnPoolQueueBean;
    }

    private static PoolRequestQueue queueModelToView(MoverQueue moverQueue) {
        PoolRequestQueue queue = new PoolRequestQueue();
        queue.setActive(moverQueue.getActive());
        queue.setMax(moverQueue.getMax());
        queue.setQueued(moverQueue.getQueued());
        return queue;
    }
}
