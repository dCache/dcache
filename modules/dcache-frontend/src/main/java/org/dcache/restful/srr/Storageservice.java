package org.dcache.restful.srr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.gson.annotations.SerializedName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Storageservice {

    private String name;
    private String id;
    private String servicetype;
    /**
     * (Required)
     */
    private String implementation;
    /**
     * (Required)
     */
    private String implementationversion;
    private List<String> capabilities = null;
    private Qualitylevel qualitylevel;
    private String message;
    private Long latestupdate;
    private Long lastupdated;
    private Storagecapacity storagecapacity;
    /**
     * (Required)
     */
    private List<Storageendpoint> storageendpoints = null;
    /**
     * (Required)
     */
    private List<Storageshare> storageshares = null;
    private List<Datastore> datastores = null;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Storageservice withName(String name) {
        this.name = name;
        return this;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Storageservice withId(String id) {
        this.id = id;
        return this;
    }

    public String getServicetype() {
        return servicetype;
    }

    public void setServicetype(String servicetype) {
        this.servicetype = servicetype;
    }

    public Storageservice withServicetype(String servicetype) {
        this.servicetype = servicetype;
        return this;
    }

    /**
     * (Required)
     */
    public String getImplementation() {
        return implementation;
    }

    /**
     * (Required)
     */
    public void setImplementation(String implementation) {
        this.implementation = implementation;
    }

    public Storageservice withImplementation(String implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * (Required)
     */
    public String getImplementationversion() {
        return implementationversion;
    }

    /**
     * (Required)
     */
    public void setImplementationversion(String implementationversion) {
        this.implementationversion = implementationversion;
    }

    public Storageservice withImplementationversion(String implementationversion) {
        this.implementationversion = implementationversion;
        return this;
    }

    public List<String> getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(List<String> capabilities) {
        this.capabilities = capabilities;
    }

    public Storageservice withCapabilities(List<String> capabilities) {
        this.capabilities = capabilities;
        return this;
    }

    public Qualitylevel getQualitylevel() {
        return qualitylevel;
    }

    public void setQualitylevel(Qualitylevel qualitylevel) {
        this.qualitylevel = qualitylevel;
    }

    public Storageservice withQualitylevel(Qualitylevel qualitylevel) {
        this.qualitylevel = qualitylevel;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Storageservice withMessage(String message) {
        this.message = message;
        return this;
    }

    public Long getLatestupdate() {
        return latestupdate;
    }

    public void setLatestupdate(Long latestupdate) {
        this.latestupdate = latestupdate;
    }

    public Storageservice withLatestupdate(long latestupdate) {
        this.latestupdate = latestupdate;
        return this;
    }

    public Long getLastupdated() {
        return lastupdated;
    }

    public void setLastupdated(Long lastupdated) {
        this.lastupdated = lastupdated;
    }

    public Storageservice withLastupdated(long lastupdated) {
        this.lastupdated = lastupdated;
        return this;
    }

    public Storagecapacity getStoragecapacity() {
        return storagecapacity;
    }

    public void setStoragecapacity(Storagecapacity storagecapacity) {
        this.storagecapacity = storagecapacity;
    }

    public Storageservice withStoragecapacity(Storagecapacity storagecapacity) {
        this.storagecapacity = storagecapacity;
        return this;
    }

    /**
     * (Required)
     */
    public List<Storageendpoint> getStorageendpoints() {
        return storageendpoints;
    }

    /**
     * (Required)
     */
    public void setStorageendpoints(List<Storageendpoint> storageendpoints) {
        this.storageendpoints = storageendpoints;
    }

    public Storageservice withStorageendpoints(List<Storageendpoint> storageendpoints) {
        this.storageendpoints = storageendpoints;
        return this;
    }

    /**
     * (Required)
     */
    public List<Storageshare> getStorageshares() {
        return storageshares;
    }

    /**
     * (Required)
     */
    public void setStorageshares(List<Storageshare> storageshares) {
        this.storageshares = storageshares;
    }

    public Storageservice withStorageshares(List<Storageshare> storageshares) {
        this.storageshares = storageshares;
        return this;
    }

    public List<Datastore> getDatastores() {
        return datastores;
    }

    public void setDatastores(List<Datastore> datastores) {
        this.datastores = datastores;
    }

    public Storageservice withDatastores(List<Datastore> datastores) {
        this.datastores = datastores;
        return this;
    }

    public enum Qualitylevel {

        @SerializedName("development") DEVELOPMENT("development"),
        @SerializedName("testing") TESTING("testing"),
        @SerializedName("pre-production") PRE_PRODUCTION("pre-production"),
        @SerializedName("production") PRODUCTION("production");
        private final String value;
        private final static Map<String, Qualitylevel> CONSTANTS = new HashMap<String, Qualitylevel>();

        static {
            for (Qualitylevel c : values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Qualitylevel(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static Qualitylevel fromValue(String value) {
            Qualitylevel constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
