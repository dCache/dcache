package org.dcache.webadmin.model.dataaccess;

import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public interface InfoDAO {

    String getXmlForStatepath(String statepath) throws DAOException;
}
