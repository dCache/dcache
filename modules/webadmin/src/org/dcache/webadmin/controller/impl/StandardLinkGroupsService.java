package org.dcache.webadmin.controller.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.dcache.webadmin.model.util.AccessLatency;
import org.dcache.webadmin.model.util.RetentionPolicy;
import org.dcache.webadmin.controller.LinkGroupsService;
import org.dcache.webadmin.controller.exceptions.LinkGroupsServiceException;
import org.dcache.webadmin.model.businessobjects.LinkGroup;
import org.dcache.webadmin.model.businessobjects.SpaceReservation;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.pages.spacetokens.beans.LinkGroupBean;
import org.dcache.webadmin.view.pages.spacetokens.beans.SpaceReservationBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Does the mapping between linkgroups and spacereservations of the model layer
 * and the beans of the view layer. It maps linkgroups allowed retention policies
 * and access latencies into the "allowed" String by using a lowercase shortcut
 * for a false and an uppercase one for a true.
 * @author jans
 */
public class StandardLinkGroupsService implements LinkGroupsService {

    private DAOFactory _daoFactory;
    private static final Logger _log = LoggerFactory.getLogger(
            StandardLinkGroupsService.class);

    public StandardLinkGroupsService(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    @Override
    public List<LinkGroupBean> getLinkGroups() throws LinkGroupsServiceException {
        try {
            Set<LinkGroup> linkGroups = getLinkGroupsDAO().getLinkGroups();
            _log.debug("returned link Groups: {} ", linkGroups.size());
            List<LinkGroupBean> linkGroupBeans = new ArrayList<LinkGroupBean>();
            for (LinkGroup linkGroup : linkGroups) {
                LinkGroupBean newBean = new LinkGroupBean();
                newBean.setId(linkGroup.getId());
                newBean.setAllowed(mapAllowanceFlags(linkGroup));
                newBean.setFree(linkGroup.getFree());
                newBean.setName(linkGroup.getName());
                newBean.setAvailable(linkGroup.getAvailable());
                newBean.setReserved(linkGroup.getReserved());
                newBean.setTotal(linkGroup.getTotal());
                newBean.setVos(linkGroup.getVos());
                newBean.setReservations(createReservations(linkGroup));
                linkGroupBeans.add(newBean);
            }
            Collections.sort(linkGroupBeans);
            _log.debug("returned linkGroupBeans: {}", linkGroupBeans.size());
            return linkGroupBeans;
        } catch (DAOException e) {
            throw new LinkGroupsServiceException(e);
        }
    }

    private LinkGroupsDAO getLinkGroupsDAO() {
        return _daoFactory.getLinkGroupsDAO();
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    private String mapAllowanceFlags(LinkGroup linkGroup) {
        String result = (linkGroup.isOnlineAllowed()
                ? AccessLatency.ONLINE.getShortcut().toUpperCase()
                : AccessLatency.ONLINE.getShortcut().toLowerCase()) +
                (linkGroup.isNearlineAllowed()
                ? AccessLatency.NEARLINE.getShortcut().toUpperCase()
                : AccessLatency.NEARLINE.getShortcut().toLowerCase()) +
                (linkGroup.isReplicaAllowed()
                ? RetentionPolicy.REPLICA.getShortcut().toUpperCase()
                : RetentionPolicy.REPLICA.getShortcut().toLowerCase()) +
                (linkGroup.isCustodialAllowed()
                ? RetentionPolicy.CUSTODIAL.getShortcut().toUpperCase()
                : RetentionPolicy.CUSTODIAL.getShortcut().toLowerCase());
        return result;
    }

    private List<SpaceReservationBean> createReservations(LinkGroup linkGroup) {
        List<SpaceReservationBean> reservations = new ArrayList<SpaceReservationBean>();
        for (SpaceReservation reservation : linkGroup.getSpaceReservations()) {
            SpaceReservationBean newReservation = new SpaceReservationBean();
            newReservation.setAllocatedSpace(reservation.getAllocatedSpace());
            newReservation.setCreated(reservation.getCreated());
            newReservation.setDescription(reservation.getDescription());
//                    newReservation.setExpiration(reservation.g)TODO Info doesn't provide
            newReservation.setId(reservation.getId());
//                    newReservation.setLifetime(reservation.ge)TODO Info doesn't provide
            newReservation.setLinkGroupRef(reservation.getLinkGroupRef());
            newReservation.setSize(reservation.getTotalSpace());
            newReservation.setState(reservation.getState());
            newReservation.setStorage(mapReservationAllowanceFlags(reservation));
            newReservation.setUsedSpace(reservation.getUsedSpace());
            newReservation.setVogroup(reservation.getVogroup());
        }
        return reservations;
    }

    private String mapReservationAllowanceFlags(SpaceReservation reservation) {
        String result = mapAccessLatency(reservation.getAccessLatency()) +
                mapRetentionPolicy(reservation.getRetentionPolicy());
        return result;
    }

    private String mapAccessLatency(AccessLatency accessLatency) {
        if (accessLatency != null) {
            return accessLatency.getShortcut();
        }
        return "";
    }

    private String mapRetentionPolicy(RetentionPolicy retentionPolicy) {
        if (retentionPolicy != null) {
            return retentionPolicy.getShortcut();
        }
        return "";
    }
}
