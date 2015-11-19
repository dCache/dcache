package org.dcache.webadmin.controller;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.TapeTransfersServiceException;
import org.dcache.webadmin.view.pages.tapetransferqueue.beans.RestoreBean;

/**
 *
 * @author jans
 */
public interface TapeTransfersService {

    List<RestoreBean> getRestores() throws TapeTransfersServiceException;
}
