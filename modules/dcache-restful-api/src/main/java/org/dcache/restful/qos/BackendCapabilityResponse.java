package org.dcache.restful.qos;
/**
 * QoS response object
 */
public class BackendCapabilityResponse {
    private String status = "200";
    private String message = "successful";
    private String qos;
    private String target;
    private BackendCapability backendCapability;
    public String getStatus() {
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }
    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }
    public BackendCapability getBackendCapability() {
        return backendCapability;
    }
    public void setBackendCapability(BackendCapability backendCapability) {
        this.backendCapability = backendCapability;
    }
    public String getQos() {
        return qos;
    }
    public void setQoS(String qos) {
        this.qos = qos;
    }
    public String getTargetQoS() {
        return target;
    }
    public void setTargetQoS(String targetQoS) {
        this.target = targetQoS;
    }
}