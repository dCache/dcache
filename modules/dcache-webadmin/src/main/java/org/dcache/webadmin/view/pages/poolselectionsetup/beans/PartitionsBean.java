package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import java.io.Serializable;
import java.util.Map;

/**
 *
 * @author jans
 */
public class PartitionsBean implements Serializable {

    private static final long serialVersionUID = 1078540091237493158L;
    private String _partitionName = "";
    private Map<String, String> _properties;

    public Map<String, String> getProperties() {
        return _properties;
    }

    public void setProperties(Map<String, String> properties) {
        _properties = properties;
    }

    public String getPartitionName() {
        return _partitionName;
    }

    public void setPartitionName(String partitionName) {
        _partitionName = partitionName;
    }
}
