package org.dcache.webadmin.controller;

import org.dcache.webadmin.controller.exceptions.InfoServiceException;

/**
 *
 * @author jans
 */
public interface InfoService {

    public String getXmlForStatepath(String statepath) throws InfoServiceException;

}
