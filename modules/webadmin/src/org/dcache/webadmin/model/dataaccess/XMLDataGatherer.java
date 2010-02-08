package org.dcache.webadmin.model.dataaccess;

import java.util.List;
import org.dcache.webadmin.model.exceptions.DataGatheringException;

/**
 * Represents XML Data-gathering possibilities from a given state Path
 * @author jans
 */
public interface XMLDataGatherer {

    public String getXML(List<String> pathElements) throws DataGatheringException;
}
