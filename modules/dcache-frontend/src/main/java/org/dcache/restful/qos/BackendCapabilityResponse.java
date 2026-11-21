package org.dcache.restful.qos;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Information about a particular quality of service.")
public class BackendCapabilityResponse {

    @Schema(description = "The HTTP status code.", requiredMode = Schema.RequiredMode.REQUIRED)
    private String status = "200";

    @Schema(description = "The message corresponding to the HTTP status code.",
          requiredMode = Schema.RequiredMode.REQUIRED)
    private String message = "successful";

    @Schema(description = "Additional information about this quality of service.")
    private String qos;

    @Schema(description = "The current quality of service.",
          requiredMode = Schema.RequiredMode.REQUIRED)
    private String target;

    @Schema(description = "The target quality of service when a file is "
          + "transitioning between different quality of services.")
    private BackendCapability backendCapability;

    public BackendCapability getBackendCapability() {
        return backendCapability;
    }

    public String getMessage() {
        return message;
    }

    public String getQos() {
        return qos;
    }

    public String getStatus() {
        return status;
    }

    public String getTargetQoS() {
        return target;
    }

    public void setBackendCapability(BackendCapability backendCapability) {
        this.backendCapability = backendCapability;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setQoS(String qos) {
        this.qos = qos;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setTargetQoS(String targetQoS) {
        this.target = targetQoS;
    }
}