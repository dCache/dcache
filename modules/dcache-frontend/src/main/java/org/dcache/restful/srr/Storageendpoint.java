package org.dcache.restful.srr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Storageendpoint {

    /**
     * (Required)
     */
    private String name;
    private String id;
    /**
     * (Required)
     */
    private String endpointurl;
    /**
     * (Required)
     */
    private String interfacetype;
    private String interfaceversion;
    private List<String> capabilities = null;
    private Qualitylevel qualitylevel;
    /**
     * (Required)
     */
    private List<String> assignedshares = null;
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

    public Storageendpoint withName(String name) {
        this.name = name;
        return this;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Storageendpoint withId(String id) {
        this.id = id;
        return this;
    }

    /**
     * (Required)
     */
    public String getEndpointurl() {
        return endpointurl;
    }

    /**
     * (Required)
     */
    public void setEndpointurl(String endpointurl) {
        this.endpointurl = endpointurl;
    }

    public Storageendpoint withEndpointurl(String endpointurl) {
        this.endpointurl = endpointurl;
        return this;
    }

    /**
     * (Required)
     */
    public String getInterfacetype() {
        return interfacetype;
    }

    /**
     * (Required)
     */
    public void setInterfacetype(String interfacetype) {
        this.interfacetype = interfacetype;
    }

    public Storageendpoint withInterfacetype(String interfacetype) {
        this.interfacetype = interfacetype;
        return this;
    }

    public String getInterfaceversion() {
        return interfaceversion;
    }

    public void setInterfaceversion(String interfaceversion) {
        this.interfaceversion = interfaceversion;
    }

    public Storageendpoint withInterfaceversion(String interfaceversion) {
        this.interfaceversion = interfaceversion;
        return this;
    }

    public List<String> getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(List<String> capabilities) {
        this.capabilities = capabilities;
    }

    public Storageendpoint withCapabilities(List<String> capabilities) {
        this.capabilities = capabilities;
        return this;
    }

    public Qualitylevel getQualitylevel() {
        return qualitylevel;
    }

    public void setQualitylevel(Qualitylevel qualitylevel) {
        this.qualitylevel = qualitylevel;
    }

    public Storageendpoint withQualitylevel(Qualitylevel qualitylevel) {
        this.qualitylevel = qualitylevel;
        return this;
    }

    /**
     * (Required)
     */
    public List<String> getAssignedshares() {
        return assignedshares;
    }

    /**
     * (Required)
     */
    public void setAssignedshares(List<String> assignedshares) {
        this.assignedshares = assignedshares;
    }

    public Storageendpoint withAssignedshares(List<String> assignedshares) {
        this.assignedshares = assignedshares;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Storageendpoint withMessage(String message) {
        this.message = message;
        return this;
    }

    public enum Qualitylevel {

        DEVELOPMENT("development"),
        TESTING("testing"),
        PRE_PRODUCTION("pre-production"),
        PRODUCTION("production");
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

        public String value() {
            return this.value;
        }

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
