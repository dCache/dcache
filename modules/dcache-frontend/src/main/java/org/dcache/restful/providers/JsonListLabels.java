package org.dcache.restful.providers;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is needed to mapping Labels,
 */
@ApiModel(description = "Specifies the attributes of a given file.")
public class JsonListLabels {


    @ApiModelProperty("All existing labels.")
    private Set<String> labels;


    public void setLabels(Set<String> labelnames) {
        if (labelnames == null) {
            return;
        }
        if (labels == null) {
            labels = new HashSet();
        }
        labels.addAll(labelnames);
    }

    public Set<String> getLabels() {
        return labels == null ? new HashSet() : labels;
    }
}