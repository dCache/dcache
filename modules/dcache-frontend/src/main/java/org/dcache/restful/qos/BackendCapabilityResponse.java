package org.dcache.restful.qos;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description = "Information about a particular quality of service.")
public class BackendCapabilityResponse {

    @ApiModelProperty(value = "The HTTP status code.", required = true)
    private String status = "200";

    @ApiModelProperty(value = "The message corresponding to the HTTP status code.",
                    required = true)
    private String message = "successful";

    @ApiModelProperty("Additional information about this quality of service.")
    private String qos;

    @ApiModelProperty(value = "The current quality of service.", required = true)
    private String target;

    @ApiModelProperty("The target quality of service when a file is "
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