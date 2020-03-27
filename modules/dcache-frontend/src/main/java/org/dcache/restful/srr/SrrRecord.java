package org.dcache.restful.srr;


/**
 * Storage Resource Reporting Schema
 * <p>
 */
public class SrrRecord {

    /**
     * (Required)
     */
    private Storageservice storageservice;

    /**
     * (Required)
     */
    public Storageservice getStorageservice() {
        return storageservice;
    }

    /**
     * (Required)
     */
    public void setStorageservice(Storageservice storageservice) {
        this.storageservice = storageservice;
    }

    public SrrRecord withStorageservice(Storageservice storageservice) {
        this.storageservice = storageservice;
        return this;
    }

}
