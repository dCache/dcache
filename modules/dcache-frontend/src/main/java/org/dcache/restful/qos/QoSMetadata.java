package org.dcache.restful.qos;

import java.util.List;

/**
 * This class represents data describing specific QoS based on CDMI specification.
 */
public class QoSMetadata {

    private String cdmi_data_redundancy_provided;
    private List<String> cdmi_geographic_placement_provided;
    private String cdmi_latency_provided;

    public QoSMetadata(String cdmi_data_redundancy_provided,
                       List<String> cdmi_geographic_placement_provided,
                       String cdmi_latency_provided) {
        this.cdmi_data_redundancy_provided = cdmi_data_redundancy_provided;
        this.cdmi_geographic_placement_provided = cdmi_geographic_placement_provided;
        this.cdmi_latency_provided = cdmi_latency_provided;
    }

    //TODO clean up (underscores)
    public String getCdmi_data_redundancy_provided() {
        return cdmi_data_redundancy_provided;
    }

    public void setCdmi_data_redundancy_provided(String cdmi_data_redundancy_provided) {
        this.cdmi_data_redundancy_provided = cdmi_data_redundancy_provided;
    }

    public String getCdmi_latency_provided() {
        return cdmi_latency_provided;
    }

    public void setCdmi_latency_provided(String cdmi_latency_provided) {
        this.cdmi_latency_provided = cdmi_latency_provided;
    }

    public List<String> getCdmi_geographic_placement_provided() {
        return cdmi_geographic_placement_provided;
    }

    public void setCdmi_geographic_placement_provided(List<String> cdmi_geographic_placement_provided) {
        this.cdmi_geographic_placement_provided = cdmi_geographic_placement_provided;
    }

}
