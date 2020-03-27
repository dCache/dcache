package org.dcache.restful.srr;

import java.util.HashMap;
import java.util.Map;

public class Datastore {

    /**
     * (Required)
     */
    private String name;
    private String id;
    /**
     * (Required)
     */
    private Datastoretype datastoretype;
    /**
     * (Required)
     */
    private Accesslatency accesslatency;
    /**
     * (Required)
     */
    private long totalsize;
    /**
     * (Required)
     */
    private String vendor;
    /**
     * (Required)
     */
    private Bandwith bandwith;
    private String message;

    /**
     * (Required)
     */
    public String getName() {
        return name;
    }

    /**
     * (Required)
     */
    public void setName(String name) {
        this.name = name;
    }

    public Datastore withName(String name) {
        this.name = name;
        return this;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Datastore withId(String id) {
        this.id = id;
        return this;
    }

    /**
     * (Required)
     */
    public Datastoretype getDatastoretype() {
        return datastoretype;
    }

    /**
     * (Required)
     */
    public void setDatastoretype(Datastoretype datastoretype) {
        this.datastoretype = datastoretype;
    }

    public Datastore withDatastoretype(Datastoretype datastoretype) {
        this.datastoretype = datastoretype;
        return this;
    }

    /**
     * (Required)
     */
    public Accesslatency getAccesslatency() {
        return accesslatency;
    }

    /**
     * (Required)
     */
    public void setAccesslatency(Accesslatency accesslatency) {
        this.accesslatency = accesslatency;
    }

    public Datastore withAccesslatency(Accesslatency accesslatency) {
        this.accesslatency = accesslatency;
        return this;
    }

    /**
     * (Required)
     */
    public long getTotalsize() {
        return totalsize;
    }

    /**
     * (Required)
     */
    public void setTotalsize(long totalsize) {
        this.totalsize = totalsize;
    }

    public Datastore withTotalsize(long totalsize) {
        this.totalsize = totalsize;
        return this;
    }

    /**
     * (Required)
     */
    public String getVendor() {
        return vendor;
    }

    /**
     * (Required)
     */
    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public Datastore withVendor(String vendor) {
        this.vendor = vendor;
        return this;
    }

    /**
     * (Required)
     */
    public Bandwith getBandwith() {
        return bandwith;
    }

    /**
     * (Required)
     */
    public void setBandwith(Bandwith bandwith) {
        this.bandwith = bandwith;
    }

    public Datastore withBandwith(Bandwith bandwith) {
        this.bandwith = bandwith;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Datastore withMessage(String message) {
        this.message = message;
        return this;
    }

    public enum Accesslatency {

        ONLINE("online"),
        OFFLINE("offline"),
        NEARLINE("nearline");
        private final String value;
        private final static Map<String, Accesslatency> CONSTANTS = new HashMap<String, Accesslatency>();

        static {
            for (Accesslatency c : values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Accesslatency(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        public String value() {
            return this.value;
        }

        public static Accesslatency fromValue(String value) {
            Accesslatency constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }
    }

    public enum Datastoretype {

        DISK("disk"),
        TAPE("tape"),
        CLOUD("cloud");
        private final String value;
        private final static Map<String, Datastoretype> CONSTANTS = new HashMap<String, Datastoretype>();

        static {
            for (Datastoretype c : values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Datastoretype(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        public String value() {
            return this.value;
        }

        public static Datastoretype fromValue(String value) {
            Datastoretype constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
