package org.dcache.restful.srr;


public class Storagecapacity {

    /**
     * (Required)
     */
    private Online online;
    private Offline offline;
    private Nearline nearline;

    /**
     * (Required)
     */
    public Online getOnline() {
        return online;
    }

    /**
     * (Required)
     */
    public void setOnline(Online online) {
        this.online = online;
    }

    public Storagecapacity withOnline(Online online) {
        this.online = online;
        return this;
    }

    public Offline getOffline() {
        return offline;
    }

    public void setOffline(Offline offline) {
        this.offline = offline;
    }

    public Storagecapacity withOffline(Offline offline) {
        this.offline = offline;
        return this;
    }

    public Nearline getNearline() {
        return nearline;
    }

    public void setNearline(Nearline nearline) {
        this.nearline = nearline;
    }

    public Storagecapacity withNearline(Nearline nearline) {
        this.nearline = nearline;
        return this;
    }

}
