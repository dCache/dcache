package org.dcache.webadmin.controller.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionEntity;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;

import org.dcache.poolmanager.Partition;
import org.dcache.webadmin.controller.PoolSelectionSetupService;
import org.dcache.webadmin.controller.exceptions.PoolSelectionSetupServiceException;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.DCacheEntityContainerBean;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.EntityReference;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.EntityType;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.IORequest;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.LinkEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.MatchBean;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.PartitionsBean;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.PoolEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.PoolGroupEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.UGroupEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.UnitEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.panels.simulatediorequest.IoDirections;

/**
 *
 * @author jans
 */
public class StandardPoolSelectionSetupService implements PoolSelectionSetupService {

    private static final Logger _log = LoggerFactory.getLogger(StandardPoolSelectionSetupService.class);
    private DAOFactory _daoFactory;

    public StandardPoolSelectionSetupService(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    @Override
    public List<PartitionsBean> getPartitions() throws PoolSelectionSetupServiceException {
        try {
            Map<String, Partition> partitions = getPoolsDAO().getPartitions();
            return createPartitionBeans(partitions);
        } catch (DAOException ex) {
            throw new PoolSelectionSetupServiceException("could not retrive selection data", ex);
        }
    }

    @Override
    public DCacheEntityContainerBean getEntityContainer() throws PoolSelectionSetupServiceException {
        DCacheEntityContainerBean container = new DCacheEntityContainerBean();
        try {
            container.setPoolGroups(createContainerPoolGroupsFromDAO(
                    getPoolsDAO().getPoolGroups()));
            container.setUnitGroups(createContainerUnitGroupsFromDAO(
                    getPoolsDAO().getUnitGroups()));
            container.setUnits(createContainerUnitsFromDAO(getPoolsDAO().getUnits()));
            container.setLinks(createContainerLinksFromDAO(getPoolsDAO().getLinks()));
            container.setPools(createContainerPoolsFromDAO(getPoolsDAO().getPools()));
        } catch (DAOException ex) {
            throw new PoolSelectionSetupServiceException("could not retrive selection data", ex);
        }
        return container;
    }

    private Collection<SelectionPool> extractSelectionPools(Set<Pool> pools) {
        Collection<SelectionPool> returnPools = new ArrayList<>();
        for (Pool pool : pools) {
            returnPools.add(pool.getSelectionPool());
        }
        return returnPools;
    }

    private List<EntityReference> createReferencesOfType(Collection<? extends SelectionEntity> referenceSources,
            EntityType type) {
        List<EntityReference> references = new ArrayList<>();
        for (SelectionEntity source : referenceSources) {
            EntityReference newReference = new EntityReference(source.getName(), type);
            references.add(newReference);
        }
        return references;
    }

    private Collection<PoolGroupEntity> createContainerPoolGroupsFromDAO(
            Collection<SelectionPoolGroup> poolGroups) throws DAOException {
        Collection<PoolGroupEntity> entites = new HashSet<>();
        for (SelectionPoolGroup group : poolGroups) {
            PoolGroupEntity entity = new PoolGroupEntity();
            entity.setName(group.getName());
            entity.setPoolMembers(createReferencesOfType(
                    extractSelectionPools(getPoolsDAO().getPoolsOfPoolGroup(group.getName())),
                    EntityType.POOL));
            entity.setLinksTargetingUs(createReferencesOfType(
                    getPoolsDAO().getLinksPointingToPoolGroup(group.getName()),
                    EntityType.LINK));
            entites.add(entity);
        }
        return entites;
    }

    private Collection<UGroupEntity> createContainerUnitGroupsFromDAO(Collection<SelectionUnitGroup> unitGroups)
    {
        Collection<UGroupEntity> entites = new HashSet<>();
        for (SelectionUnitGroup unitGroup : unitGroups) {
            UGroupEntity entity = new UGroupEntity();
            entity.setName(unitGroup.getName());
            entity.setUnits(createReferencesOfType(
                    unitGroup.getMemeberUnits(),
                    EntityType.UNIT));
            entity.setLinks(createReferencesOfType(
                    unitGroup.getLinksPointingTo(),
                    EntityType.LINK));
            entites.add(entity);
        }
        return entites;
    }

    private Collection<UnitEntity> createContainerUnitsFromDAO(Collection<SelectionUnit> units)
    {
        Collection<UnitEntity> entites = new HashSet<>();
        for (SelectionUnit unit : units) {
            UnitEntity entity = new UnitEntity();
            entity.setName(unit.getName());
            entity.setType(unit.getUnitType());
            entity.setUnitGroups(createReferencesOfType(
                    unit.getMemberOfUnitGroups(),
                    EntityType.UNITGROUP));
            entites.add(entity);
        }
        return entites;
    }

    private Collection<LinkEntity> createContainerLinksFromDAO(Collection<SelectionLink> links)
    {
        Collection<LinkEntity> entites = new HashSet<>();
        for (SelectionLink link : links) {
            LinkEntity entity = new LinkEntity();
            entity.setName(link.getName());
            entity.setP2pPreference(link.getPreferences().getP2pPref());
            entity.setPartition(link.getTag());
            entity.setPoolGroupsPointingTo(createReferencesOfType(
                    link.getPoolGroupsPointingTo(),
                    EntityType.POOLGROUP));
            entity.setReadPreference(link.getPreferences().getReadPref());
            entity.setRestorePreference(link.getPreferences().getRestorePref());
            entity.setUnitGroupsFollowed(createReferencesOfType(
                    link.getUnitGroupsTargetedBy(),
                    EntityType.UNITGROUP));
            entity.setWritePreference(link.getPreferences().getWritePref());
            entites.add(entity);
        }
        return entites;
    }

    private Collection<PoolEntity> createContainerPoolsFromDAO(Set<Pool> pools) throws DAOException {
        Collection<PoolEntity> entites = new HashSet<>();
        for (Pool pool : pools) {
            entites.add(mapPoolToEntity(pool));
        }
        return entites;
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    private PoolsDAO getPoolsDAO() {
        return _daoFactory.getPoolsDAO();
    }

    private PoolEntity mapPoolToEntity(Pool pool) throws DAOException {
        PoolEntity newEntity = new PoolEntity();
        newEntity.setIsActive(pool.getSelectionPool().isActive());
        newEntity.setIsEnabled(pool.getSelectionPool().isEnabled());
        newEntity.setMode(pool.getSelectionPool().getPoolMode().toString());
        newEntity.setName(pool.getName());
        newEntity.setPoolGroupsBelongingTo(createReferencesOfType(
                getPoolsDAO().getPoolGroupsOfPool(pool.getName()),
                EntityType.POOLGROUP));
        newEntity.setLinksTargetingUs(createReferencesOfType(
                pool.getSelectionPool().getLinksTargetingPool(),
                EntityType.LINK));
        return newEntity;
    }

    @Override
    public List<MatchBean> getMatchForIORequest(IORequest request)
            throws PoolSelectionSetupServiceException {
        try {
            PoolPreferenceLevel[] preferences = getPoolsDAO().match(
                    mapIoDirectionsViewToModel(request.getType()),
                    request.getNetUnitName(), request.getProtocolUnitName(),
                    request.getStore(), request.getDcache(), request.getLinkGroupName());
            List<MatchBean> matches = new ArrayList<>();
            _log.debug("preferences: {}", preferences.length);
            for (PoolPreferenceLevel preference : preferences) {
                MatchBean match = new MatchBean(preference.getPoolList(), preference.getTag());
                matches.add(match);
            }
            _log.debug("matches: {}", matches.size());
            return matches;
        } catch (DAOException ex) {
            throw new PoolSelectionSetupServiceException("could not retrive selection data", ex);
        }
    }

    private DirectionType mapIoDirectionsViewToModel(IoDirections direction) {
        switch (direction) {
            case READ:
                return DirectionType.READ;
            case WRITE:
                return DirectionType.WRITE;
            case P2P:
                return DirectionType.P2P;
            case CACHE:
                return DirectionType.CACHE;
            default:
                throw new RuntimeException("IoDirection unknown " + direction);
        }
    }

    private List<PartitionsBean> createPartitionBeans(Map<String, Partition> partitions) {
        List<PartitionsBean> partitionBeans = new ArrayList<>();
        for (String name : partitions.keySet()) {
            partitionBeans.add(BeanDataMapper.partitionModelToView(name, partitions.get(name)));
        }
        return partitionBeans;
    }
}
