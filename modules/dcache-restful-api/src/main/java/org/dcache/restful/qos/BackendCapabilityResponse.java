package org.dcache.restful.qos;

/**
 * QoS response object
 */
public class BackendCapabilityResponse {

    private String status;
    private String message;

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
}
