package org.dcache.restful.srr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.gson.annotations.SerializedName;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Storageshare {

    private String name;
    private String id;
    private List<String> policyrules = null;
    private List<String> accessmode = null;
    private Accesslatency accesslatency;
    private Servingstate servingstate;
    private Retentionpolicy retentionpolicy;
    private Lifetime lifetime;
    /**
     * (Required)
     */
    private Long timestamp;
    /**
     * (Required)
     */
    private Long totalsize;
    /**
     * (Required)
     */
    private Long usedsize;
    private Long numberoffiles;
    private List<String> path = null;
    private List<String> assignedendpoints = null;
    /**
     * (Required)
     */
    private List<String> vos = null;
    private String message;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Storageshare withName(String name) {
        this.name = name;
        return this;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Storageshare withId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getPolicyrules() {
        return policyrules;
    }

    public void setPolicyrules(List<String> policyrules) {
        this.policyrules = policyrules;
    }

    public Storageshare withPolicyrules(List<String> policyrules) {
        this.policyrules = policyrules;
        return this;
    }

    public List<String> getAccessmode() {
        return accessmode;
    }

    public void setAccessmode(List<String> accessmode) {
        this.accessmode = accessmode;
    }

    public Storageshare withAccessmode(List<String> accessmode) {
        this.accessmode = accessmode;
        return this;
    }

    public Accesslatency getAccesslatency() {
        return accesslatency;
    }

    public void setAccesslatency(Accesslatency accesslatency) {
        this.accesslatency = accesslatency;
    }

    public Storageshare withAccesslatency(Accesslatency accesslatency) {
        this.accesslatency = accesslatency;
        return this;
    }

    public Servingstate getServingstate() {
        return servingstate;
    }

    public void setServingstate(Servingstate servingstate) {
        this.servingstate = servingstate;
    }

    public Storageshare withServingstate(Servingstate servingstate) {
        this.servingstate = servingstate;
        return this;
    }

    public Retentionpolicy getRetentionpolicy() {
        return retentionpolicy;
    }

    public void setRetentionpolicy(Retentionpolicy retentionpolicy) {
        this.retentionpolicy = retentionpolicy;
    }

    public Storageshare withRetentionpolicy(Retentionpolicy retentionpolicy) {
        this.retentionpolicy = retentionpolicy;
        return this;
    }

    public Lifetime getLifetime() {
        return lifetime;
    }

    public void setLifetime(Lifetime lifetime) {
        this.lifetime = lifetime;
    }

    public Storageshare withLifetime(Lifetime lifetime) {
        this.lifetime = lifetime;
        return this;
    }

    /**
     * (Required)
     */
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * (Required)
     */
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Storageshare withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * (Required)
     */
    public Long getTotalsize() {
        return totalsize;
    }

    /**
     * (Required)
     */
    public void setTotalsize(Long totalsize) {
        this.totalsize = totalsize;
    }

    public Storageshare withTotalsize(long totalsize) {
        this.totalsize = totalsize;
        return this;
    }

    /**
     * (Required)
     */
    public Long getUsedsize() {
        return usedsize;
    }

    /**
     * (Required)
     */
    public void setUsedsize(Long usedsize) {
        this.usedsize = usedsize;
    }

    public Storageshare withUsedsize(long usedsize) {
        this.usedsize = usedsize;
        return this;
    }

    public Long getNumberoffiles() {
        return numberoffiles;
    }

    public void setNumberoffiles(Long numberoffiles) {
        this.numberoffiles = numberoffiles;
    }

    public Storageshare withNumberoffiles(long numberoffiles) {
        this.numberoffiles = numberoffiles;
        return this;
    }

    public List<String> getPath() {
        return path;
    }

    public void setPath(List<String> path) {
        this.path = path;
    }

    public Storageshare withPath(List<String> path) {
        this.path = path;
        return this;
    }

    public List<String> getAssignedendpoints() {
        return assignedendpoints;
    }

    public void setAssignedendpoints(List<String> assignedendpoints) {
        this.assignedendpoints = assignedendpoints;
    }

    public Storageshare withAssignedendpoints(List<String> assignedendpoints) {
        this.assignedendpoints = assignedendpoints;
        return this;
    }

    /**
     * (Required)
     */
    public List<String> getVos() {
        return vos;
    }

    /**
     * (Required)
     */
    public void setVos(List<String> vos) {
        this.vos = vos;
    }

    public Storageshare withVos(List<String> vos) {
        this.vos = vos;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Storageshare withMessage(String message) {
        this.message = message;
        return this;
    }

    public enum Accesslatency {

        @SerializedName("online") ONLINE("online"),
        @SerializedName("offline") OFFLINE("offline"),
        @SerializedName("nearline") NEARLINE("nearline");
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

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static Accesslatency fromValue(String value) {
            Accesslatency constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

        public static Accesslatency fromValue(AccessLatency value) {

            if (value.equals(AccessLatency.ONLINE)) {
                return Accesslatency.ONLINE;
            } else if (value.equals(AccessLatency.NEARLINE)) {
                return Accesslatency.NEARLINE;
            } else {
                throw new IllegalArgumentException();
            }
        }
    }

    public enum Retentionpolicy {

        @SerializedName("nome") NONE("none"),
        @SerializedName("intermediate") INTERMEDIATE("intermediate"),
        @SerializedName("replicated") REPLICATED("replicated"),
        @SerializedName("custodial") CUSTODIAL("custodial"),
        @SerializedName("replica") REPLICA("replica");
        private final String value;
        private final static Map<String, Retentionpolicy> CONSTANTS = new HashMap<String, Retentionpolicy>();

        static {
            for (Retentionpolicy c : values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Retentionpolicy(String value) {
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
        public static Retentionpolicy fromValue(String value) {
            Retentionpolicy constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

        public static Retentionpolicy fromValue(RetentionPolicy value) {

            if (value.equals(RetentionPolicy.CUSTODIAL)) {
                return Retentionpolicy.CUSTODIAL;
            } else if (value.equals(RetentionPolicy.REPLICA)) {
                return Retentionpolicy.REPLICA;
            } else if (value.equals(RetentionPolicy.OUTPUT)) {
                return Retentionpolicy.INTERMEDIATE;
            } else {
                throw new IllegalArgumentException();
            }
        }
    }

    public enum Servingstate {

        OPEN("open"),
        CLOSED("closed"),
        DRAINING("draining");
        private final String value;
        private final static Map<String, Servingstate> CONSTANTS = new HashMap<String, Servingstate>();

        static {
            for (Servingstate c : values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Servingstate(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        public String value() {
            return this.value;
        }

        public static Servingstate fromValue(String value) {
            Servingstate constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
