package org.dcache.webadmin.controller.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.services.space.Space;

import org.dcache.webadmin.controller.LinkGroupsService;
import org.dcache.webadmin.controller.exceptions.LinkGroupsServiceException;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.pages.spacetokens.beans.LinkGroupBean;
import org.dcache.webadmin.view.pages.spacetokens.beans.SpaceReservationBean;

/**
 * Offers services for getting information on LinkGroups and Space Tokens
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
            Collection<LinkGroup> linkGroups = getLinkGroupsDAO().getLinkGroups();
            _log.debug("returned link Groups: {} ", linkGroups.size());
            Collection<Space> reservations = getLinkGroupsDAO().getSpaceReservations();
            _log.debug("returned spacereservations: {} ", reservations.size());
            List<SpaceReservationBean> reservationBeans = createReservations(reservations);
            List<LinkGroupBean> linkGroupBeans = createLinkGroups(linkGroups);
            mapReservationsToLinkGroups(reservationBeans, linkGroupBeans);
            Collections.sort(linkGroupBeans);
            _log.debug("returned linkGroupBeans: {}", linkGroupBeans.size());
            return linkGroupBeans;
        } catch (DAOException e) {
            throw new LinkGroupsServiceException(e);
        }
    }

    private List<LinkGroupBean> createLinkGroups(Collection<LinkGroup> linkGroups) {
        List<LinkGroupBean> linkGroupBeans = new ArrayList<>();
        for (LinkGroup linkGroup : linkGroups) {
            LinkGroupBean newBean = BeanDataMapper.linkGroupModelToView(linkGroup);
            linkGroupBeans.add(newBean);
        }
        return linkGroupBeans;
    }

    private LinkGroupsDAO getLinkGroupsDAO() {
        return _daoFactory.getLinkGroupsDAO();
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    private List<SpaceReservationBean> createReservations(Collection<Space> reservations) {
        List<SpaceReservationBean> reservationBeans = new ArrayList<>();
        for (Space reservation : reservations) {
            SpaceReservationBean newReservation =
                    BeanDataMapper.spaceReservationModelToView(reservation);
            reservationBeans.add(newReservation);
        }
        return reservationBeans;
    }

    private void mapReservationsToLinkGroups(List<SpaceReservationBean> reservations,
            List<LinkGroupBean> linkGroups) {
//        only those reservations that have an linkgroupref set are mapped - others
//        get lost -- normally this should not occour anyway, so skipping check,
//        if all get mapped!
        for (LinkGroupBean linkGroup : linkGroups) {
            _log.debug("Linkgroup id: {}", linkGroup.getId());
            for (SpaceReservationBean reservation : reservations) {
                _log.debug("Reservation linkgroupref: {}", reservation.getLinkGroupRef());
                if (reservation.belongsTo(linkGroup)) {
                    _log.debug("Reservation added to Linkgroup");
                    linkGroup.addSpaceReservation(reservation);
                }
            }
        }
    }
}
