package org.dcache.restful.srr;


public class Bandwith {

    /**
     * (Required)
     */
    private String network;
    /**
     * (Required)
     */
    private String disk;
    /**
     * (Required)
     */
    private String etc;

    /**
     * (Required)
     */
    public String getNetwork() {
        return network;
    }

    /**
     * (Required)
     */
    public void setNetwork(String network) {
        this.network = network;
    }

    public Bandwith withNetwork(String network) {
        this.network = network;
        return this;
    }

    /**
     * (Required)
     */
    public String getDisk() {
        return disk;
    }

    /**
     * (Required)
     */
    public void setDisk(String disk) {
        this.disk = disk;
    }

    public Bandwith withDisk(String disk) {
        this.disk = disk;
        return this;
    }

    /**
     * (Required)
     */
    public String getEtc() {
        return etc;
    }

    /**
     * (Required)
     */
    public void setEtc(String etc) {
        this.etc = etc;
    }

    public Bandwith withEtc(String etc) {
        this.etc = etc;
        return this;
    }

}
