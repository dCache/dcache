package org.dcache.restful.qos;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

@ApiModel("Information describing the storage system.")
public class BackendCapability {

    private String name;

    private List<String> transition;

    private QoSMetadata metadata;

    @ApiModelProperty("The attributes describing this quality of service.")
    public QoSMetadata getMetadata() {
        return this.metadata;
    }

    public void setMetadata(QoSMetadata metadata) {
        this.metadata = metadata;
    }


    @ApiModelProperty("The list of quality of services to which a transition "
            + "from this quality of service is supported.")
    public List<String> getTransition() {
        return transition;
    }

    public void setTransition(List<String> transitions) {
        this.transition = transitions;
    }

    @ApiModelProperty("The name of this quality of service.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
