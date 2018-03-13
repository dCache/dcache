package org.dcache.restful.qos;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

@ApiModel(description ="Information describing the storage system.")
public class BackendCapability {

    @ApiModelProperty("The name of this quality of service.")
    private String name;

    @ApiModelProperty("The list of quality of services to which a transition "
                    + "from this quality of service is supported.")
    private List<String> transition;

    @ApiModelProperty("The attributes describing this quality of service.")
    private QoSMetadata metadata;

    public QoSMetadata getMetadata() {
        return this.metadata;
    }

    public String getName() {
        return name;
    }

    public List<String> getTransition() {
        return transition;
    }

    public void setMetadata(QoSMetadata metadata) {
        this.metadata = metadata;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTransition(List<String> transitions) {
        this.transition = transitions;
    }
}
