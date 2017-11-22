package org.dcache.restful.qos;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel("Information about a particular quality of service.")
public class BackendCapabilityResponse {
    private String status = "200";
    private String message = "successful";
    private String qos;
    private String target;
    private BackendCapability backendCapability;

    @ApiModelProperty(value = "The HTTP status code.", required = true)
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @ApiModelProperty(value = "The message corresponding to the HTTP status code.",
            required = true)
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @ApiModelProperty("Additional information about this quality of service.")
    public BackendCapability getBackendCapability() {
        return backendCapability;
    }

    public void setBackendCapability(BackendCapability backendCapability) {
        this.backendCapability = backendCapability;
    }

    @ApiModelProperty(value = "The current quality of service.", required = true)
    public String getQos() {
        return qos;
    }

    public void setQoS(String qos) {
        this.qos = qos;
    }

    @ApiModelProperty("The target quality of service when a file is "
            + "transitioning between different quality of services.")
    public String getTargetQoS() {
        return target;
    }

    public void setTargetQoS(String targetQoS) {
        this.target = targetQoS;
    }
}