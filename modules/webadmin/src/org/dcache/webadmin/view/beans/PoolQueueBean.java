package org.dcache.webadmin.view.beans;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class PoolQueueBean implements Comparable, Serializable {

    private String _name = "";
    private String _domainName = "";
    private PoolRequestQueue _movers = new PoolRequestQueue();
    private PoolRequestQueue _restores = new PoolRequestQueue();
    private PoolRequestQueue _stores = new PoolRequestQueue();
    private PoolRequestQueue _p2pserver = new PoolRequestQueue();
    private PoolRequestQueue _p2pclient = new PoolRequestQueue();
    private PoolRequestQueue _p2p = new PoolRequestQueue();
    private PoolRequestQueue _regular = new PoolRequestQueue();
    private static final Logger _log = LoggerFactory.getLogger(PoolQueueBean.class);

    public PoolQueueBean() {
    }

    public String getDomainName() {
        return _domainName;
    }

    public void setDomainName(String domainName) {
        _domainName = domainName;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public PoolRequestQueue getMovers() {
        return _movers;
    }

    public void setMovers(PoolRequestQueue movers) {
        _movers = movers;
    }

    public PoolRequestQueue getP2p() {
        return _p2p;
    }

    public void setP2p(PoolRequestQueue p2p) {
        _p2p = p2p;
    }

    public PoolRequestQueue getP2pclient() {
        return _p2pclient;
    }

    public void setP2pclient(PoolRequestQueue p2pclient) {
        _p2pclient = p2pclient;
    }

    public PoolRequestQueue getP2pserver() {
        return _p2pserver;
    }

    public void setP2pserver(PoolRequestQueue p2pserver) {
        _p2pserver = p2pserver;
    }

    public PoolRequestQueue getRegular() {
        return _regular;
    }

    public void setRegular(PoolRequestQueue regular) {
        _regular = regular;
    }

    public PoolRequestQueue getRestores() {
        return _restores;
    }

    public void setRestores(PoolRequestQueue restores) {
        _restores = restores;
    }

    public PoolRequestQueue getStores() {
        return _stores;
    }

    public void setStores(PoolRequestQueue stores) {
        _stores = stores;
    }

    public void addPoolQueue(PoolQueueBean queueToAdd) {
        if (queueToAdd == null) {
            throw new IllegalArgumentException();
        }
        _stores.addQueue(queueToAdd.getStores());
        _restores.addQueue(queueToAdd.getRestores());
        _regular.addQueue(queueToAdd.getRegular());
        _p2pserver.addQueue(queueToAdd.getP2pserver());
        _p2pclient.addQueue(queueToAdd.getP2pclient());
        _p2p.addQueue(queueToAdd.getP2p());
        _movers.addQueue(queueToAdd.getMovers());
    }

    public int compareTo(Object other) {

        if (other == null) {
            throw new NullPointerException();
        }
//      throws ClassCastException if wrong object is delivered, according to
//      specification
        PoolQueueBean otherBean = (PoolQueueBean) other;
        return this.getName().compareTo(otherBean.getName());
    }
}
