package org.dcache.webadmin.model.dataaccess;

import org.dcache.webadmin.model.exceptions.DataGatheringException;

/**
 * Represents XML Data-gathering possibilities from a given state Path
 * @author jans
 */
public interface XMLDataGatherer {

    String getXML() throws DataGatheringException;
}
