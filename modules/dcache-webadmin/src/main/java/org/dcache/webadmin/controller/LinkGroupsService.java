package org.dcache.webadmin.controller;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.LinkGroupsServiceException;
import org.dcache.webadmin.view.pages.spacetokens.beans.LinkGroupBean;

/**
 * Service concerning LinkGroups. Supposed to be the man in the middle between
 * model business objects and view beanobjects
 * @author jans
 */
public interface LinkGroupsService {

    List<LinkGroupBean> getLinkGroups() throws LinkGroupsServiceException;
}
