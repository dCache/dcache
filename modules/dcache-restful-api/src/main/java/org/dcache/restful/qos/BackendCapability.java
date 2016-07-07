package org.dcache.restful.qos;


import java.util.List;

/**
 * Data describing specific QoS.
 */
public class BackendCapability {


    private String name;

    private List<String> transition;

    private QoSMetadata metadata;


    public QoSMetadata getMetadata() {
        return this.metadata;
    }

    public void setMetadata(QoSMetadata metadata) {
        this.metadata = metadata;
    }


    public List<String> getTransition() {
        return transition;
    }

    public void setTransition(List<String> transitions) {
        this.transition = transitions;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


}
