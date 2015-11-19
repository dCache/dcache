package org.dcache.webadmin.controller;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.CellsServiceException;
import org.dcache.webadmin.view.beans.CellServicesBean;

/**
 * All methods  concerning cell-Services and CellServicesBeans
 * @author jans
 */
public interface CellsService {

    List<CellServicesBean> getCellServicesBeans()
            throws CellsServiceException;
}
