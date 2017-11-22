package org.dcache.restful.qos;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

@ApiModel("Attributes of a specific Quality of Service, based on CDMI specification.")
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
    @ApiModelProperty("Number of copies of each file.")
    public String getCdmi_data_redundancy_provided() {
        return cdmi_data_redundancy_provided;
    }

    public void setCdmi_data_redundancy_provided(String cdmi_data_redundancy_provided) {
        this.cdmi_data_redundancy_provided = cdmi_data_redundancy_provided;
    }

    @ApiModelProperty("Time taken to receive the first byte, in milliseconds.")
    public String getCdmi_latency_provided() {
        return cdmi_latency_provided;
    }

    public void setCdmi_latency_provided(String cdmi_latency_provided) {
        this.cdmi_latency_provided = cdmi_latency_provided;
    }

    @ApiModelProperty("List of geographic locations where the file resides, "
            + "using ISO 3166 codes.")
    public List<String> getCdmi_geographic_placement_provided() {
        return cdmi_geographic_placement_provided;
    }

    public void setCdmi_geographic_placement_provided(List<String> cdmi_geographic_placement_provided) {
        this.cdmi_geographic_placement_provided = cdmi_geographic_placement_provided;
    }
}
