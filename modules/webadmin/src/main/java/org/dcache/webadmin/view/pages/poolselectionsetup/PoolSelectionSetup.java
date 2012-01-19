package org.dcache.webadmin.view.pages.poolselectionsetup;

import java.util.ArrayList;
import java.util.List;
import org.apache.wicket.markup.html.WebMarkupContainerWithAssociatedMarkup;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.EmptyPanel;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.markup.html.panel.Fragment;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.markup.repeater.data.GridView;
import org.apache.wicket.markup.repeater.data.IDataProvider;
import org.apache.wicket.markup.repeater.data.ListDataProvider;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.controller.PoolSelectionSetupService;
import org.dcache.webadmin.controller.exceptions.PoolSelectionSetupServiceException;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.DCacheEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.DCacheEntityContainerBean;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.EntityReference;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.LinkEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.panels.partitions.PartitionsPanel;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.PartitionsBean;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.PoolEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.PoolGroupEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.UGroupEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.UnitEntity;
import org.dcache.webadmin.view.pages.poolselectionsetup.panels.simulatediorequest.SimulatedIORequestPanel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class PoolSelectionSetup extends BasePage {

    private static final String EMPTY_STRING = "";
    private static final Logger _log = LoggerFactory.getLogger(PoolSelectionSetup.class);
    private static final String RESULT_PANEL_ID = "resultPanel";
    private static final String PARTICULAR_PROPERTIES_ID = "particularProperties";
    private WebMarkupContainerWithAssociatedMarkup _results =
            new EmptyPanel(RESULT_PANEL_ID);
    private DCacheEntityContainerBean _entityContainer = new DCacheEntityContainerBean();

    public PoolSelectionSetup() {
        super();
        retrieveEntityContainer();
        addMarkup();
    }

    private void addMarkup() {
        _log.debug("addmarkup called");
        add(new FeedbackPanel("feedback"));
        add(new PartitonsLink("partitionsLink"));
        add(new PoolsLink("poolsLink"));
        add(new PoolGroupsLink("poolGroupsLink"));
        add(new SelectionLink("selectionLink"));
        add(new SelectionGroupsLink("selectionGroupsLink"));
        add(new LinksLink("linksLink"));
        add(new LinkListLink("linkListLink"));
        add(new MatchLink("matchLink"));
        add(_results);
    }

    private PoolSelectionSetupService getPoolSelectionSetupService() {
        return getWebadminApplication().getPoolSelectionSetupService();
    }

    private void retrieveEntityContainer() {
        try {
            _entityContainer = getPoolSelectionSetupService().getEntityContainer();
        } catch (PoolSelectionSetupServiceException ex) {
            error("No Data available yet, please reload page: " + ex.getMessage());
            _log.debug("no Data: " + ex.getMessage());
        }
    }

    public Link getLinkToPool(String linkId, String name) {
        PoolEntity entity = _entityContainer.getPool(name);
        if (entity != null) {
            return new ParticularEntityLink(linkId, entity);
        } else {
            return new Link(linkId) {

                @Override
                public void onClick() {
//                do nothing on purpose
                }
            };
        }
    }

    private class ParticularEntityLink<T extends DCacheEntity> extends Link {

        private final T _entity;

        public ParticularEntityLink(String id, T entity) {
            super(id);
            _entity = entity;
        }

        @Override
        public void onClick() {
            Fragment particularEntity =
                    new ParticularEntityFragment<T>(
                    RESULT_PANEL_ID, _entity);
            _results.replaceWith(particularEntity);
            _results = particularEntity;
        }
    }

    private class EntityListShowingFragment<T extends DCacheEntity> extends Fragment {

        private static final int ENTITIES_PER_ROW = 8;

        public EntityListShowingFragment(String id, List<T> entities, String title) {
            super(id, "entityListShowingFragment", PoolSelectionSetup.this);
            add(new Label("listTitle", title));
            IDataProvider<T> dataProvider =
                    new ListDataProvider<T>(entities);
            GridView<T> grid = new GridView<T>("entityRows",
                    dataProvider) {

                @Override
                protected void populateEmptyItem(Item item) {
                    Link link = new Link("link") {

                        @Override
                        public void onClick() {
//                           do nothing is on purpose - just an empty column
                        }
                    };
                    link.add(new Label("name", EMPTY_STRING));
                    item.add(link);
                }

                @Override
                protected void populateItem(final Item item) {
                    final T entity = (T) item.getModelObject();
                    ParticularEntityLink<T> link = new ParticularEntityLink<T>("link", entity);
                    link.add(new Label("name", entity.getName()));
                    item.add(link);
                }
            };
            grid.setColumns(Math.min(ENTITIES_PER_ROW, entities.size()));
            add(grid);
        }
    }

    private class ParticularEntityFragment<T extends DCacheEntity> extends Fragment {

        private String entityTitle = EMPTY_STRING;
        private String firstReferenceTitle = EMPTY_STRING;
        private String secondReferenceTitle = EMPTY_STRING;

        public ParticularEntityFragment(String id, T entity) {
            super(id, "particularEntityFragment", PoolSelectionSetup.this);
            add(new Label("entityTitle", new PropertyModel<String>(
                    this, "entityTitle")));
            add(getParticularFragmentAndSetTitles(entity));
            List<DCacheEntity> firstReferences =
                    extractEntitiesFromContainer(entity.getFirstReferences());
            List<DCacheEntity> secondReferences =
                    extractEntitiesFromContainer(entity.getSecondReferences());
            entityTitle = getStringResource(
                    entity.getSingleEntityViewTitleResource()) + " " + entity.getName();
            firstReferenceTitle = entity.getFirstreferenceDescription();
            secondReferenceTitle = entity.getSecondReferenceDescription();
            add(new EntityListShowingFragment("firstLinkList",
                    firstReferences, firstReferenceTitle));
            add(new EntityListShowingFragment("secondLinkList",
                    secondReferences, secondReferenceTitle));
        }

        private List<DCacheEntity> extractEntitiesFromContainer(List<EntityReference> references) {
            List<DCacheEntity> entites = new ArrayList<DCacheEntity>();
            for (EntityReference ref : references) {
                entites.add(_entityContainer.getEntity(ref.getName(), ref.getEntityType()));
            }
            return entites;
        }

        private Fragment getParticularFragmentAndSetTitles(DCacheEntity entity) {

            if (entity instanceof PoolEntity) {
                PoolEntity pool = (PoolEntity) entity;
                return new PoolFragment(PARTICULAR_PROPERTIES_ID, pool);
            } else if (entity instanceof PoolGroupEntity) {
                PoolGroupEntity poolGroup = (PoolGroupEntity) entity;
                return new PoolGroupFragment(PARTICULAR_PROPERTIES_ID, poolGroup);
            } else if (entity instanceof LinkEntity) {
                LinkEntity link = (LinkEntity) entity;
                return new LinkFragment(PARTICULAR_PROPERTIES_ID, link);
            } else if (entity instanceof UnitEntity) {
                UnitEntity unit = (UnitEntity) entity;
                return new UnitFragment(PARTICULAR_PROPERTIES_ID, unit);
            } else if (entity instanceof UGroupEntity) {
                UGroupEntity unitGroup = (UGroupEntity) entity;
                return new UnitGroupFragment(PARTICULAR_PROPERTIES_ID, unitGroup);
            } else {
                throw new IllegalArgumentException();
            }
        }
    }

    private class PartitonsLink extends Link {

        public PartitonsLink(String id) {
            super(id);
        }

        @Override
        public void onClick() {
            try {
                List<PartitionsBean> partitions = getPoolSelectionSetupService().getPartitions();
                Panel resultPanel = new PartitionsPanel(RESULT_PANEL_ID,
                        new CompoundPropertyModel<List<PartitionsBean>>(partitions));
                _results.replaceWith(resultPanel);
                _results = resultPanel;
            } catch (PoolSelectionSetupServiceException e) {
                this.error("failed to get Partitions: {}" + e.getMessage());
                _log.debug("Exception while getting Partitions {}", e);
            }
        }
    }

    private class PoolsLink extends Link {

        public PoolsLink(String id) {
            super(id);
        }

        @Override
        public void onClick() {
            Fragment results = new EntityListShowingFragment<PoolEntity>(
                    RESULT_PANEL_ID, _entityContainer.getPools(), getStringResource(
                    "pools.header"));
            _results.replaceWith(results);
            _results = results;
        }
    }

    private class PoolGroupsLink extends Link {

        public PoolGroupsLink(String id) {
            super(id);
        }

        @Override
        public void onClick() {
            Fragment results = new EntityListShowingFragment<PoolGroupEntity>(
                    RESULT_PANEL_ID, _entityContainer.getPoolGroups(), getStringResource(
                    "poolGroups.header"));
            _results.replaceWith(results);
            _results = results;
        }
    }

    private class SelectionLink extends Link {

        public SelectionLink(String id) {
            super(id);
        }

        @Override
        public void onClick() {
            Fragment results = new EntityListShowingFragment<UnitEntity>(
                    RESULT_PANEL_ID, _entityContainer.getUnits(), getStringResource(
                    "units.header"));
            _results.replaceWith(results);
            _results = results;
        }
    }

    private class SelectionGroupsLink extends Link {

        public SelectionGroupsLink(String id) {
            super(id);
        }

        @Override
        public void onClick() {
            Fragment results = new EntityListShowingFragment<UGroupEntity>(
                    RESULT_PANEL_ID, _entityContainer.getUnitGroups(), getStringResource(
                    "unitGroups.header"));
            _results.replaceWith(results);
            _results = results;

        }
    }

    private class LinksLink extends Link {

        public LinksLink(String id) {
            super(id);
        }

        @Override
        public void onClick() {
            Fragment results = new EntityListShowingFragment<LinkEntity>(
                    RESULT_PANEL_ID, _entityContainer.getLinks(), getStringResource(
                    "links.header"));
            _results.replaceWith(results);
            _results = results;

        }
    }

    private class LinkListLink extends Link {

        public LinkListLink(String id) {
            super(id);
        }

        @Override
        public void onClick() {
            Fragment results = new LinkListFragment(RESULT_PANEL_ID, _entityContainer.getLinks());
            _results.replaceWith(results);
            _results = results;
        }
    }

    private class MatchLink extends Link {

        public MatchLink(String id) {
            super(id);
        }

        @Override
        public void onClick() {
            Panel resultPanel = new SimulatedIORequestPanel(RESULT_PANEL_ID,
                    PoolSelectionSetup.this);
            _results.replaceWith(resultPanel);
            _results = resultPanel;
        }
    }

    private class PoolFragment extends Fragment {

        public PoolFragment(String id, PoolEntity entity) {
            super(id, "poolFragment", PoolSelectionSetup.this);
            this.setDefaultModel(new CompoundPropertyModel<PoolEntity>(entity));
            add(new Label("_isEnabled"));
            add(new Label("_mode"));
            add(new Label("_isActive"));
        }
    }

    private class PoolGroupFragment extends Fragment {

        public PoolGroupFragment(String id, PoolGroupEntity entity) {
            super(id, "poolGroupFragment", PoolSelectionSetup.this);
            this.setDefaultModel(new CompoundPropertyModel<PoolGroupEntity>(entity));
            add(new Label("_name"));
        }
    }

    private class LinkFragment extends Fragment {

        public LinkFragment(String id, LinkEntity entity) {
            super(id, "linkFragment", PoolSelectionSetup.this);
            this.setDefaultModel(new CompoundPropertyModel<LinkEntity>(entity));
            add(new Label("_name"));
            add(new Label("_writePreference"));
            add(new Label("_readPreference"));
            add(new Label("_restorePreference"));
            add(new Label("_p2pPreference"));
            add(new Label("_partition"));

        }
    }

    private class UnitFragment extends Fragment {

        public UnitFragment(String id, UnitEntity entity) {
            super(id, "unitFragment", PoolSelectionSetup.this);
            this.setDefaultModel(new CompoundPropertyModel<UnitEntity>(entity));
            add(new Label("_name"));
            add(new Label("_type"));
        }
    }

    private class UnitGroupFragment extends Fragment {

        public UnitGroupFragment(String id, UGroupEntity entity) {
            super(id, "unitGroupFragment", PoolSelectionSetup.this);
            this.setDefaultModel(new CompoundPropertyModel<UGroupEntity>(entity));
            add(new Label("_name"));
        }
    }

    private class LinkListFragment extends Fragment {

        public LinkListFragment(String id, List<LinkEntity> links) {
            super(id, "linkListFragment", PoolSelectionSetup.this);
            ListView<LinkEntity> linkList = new ListView<LinkEntity>("linkListView", links) {

                @Override
                protected void populateItem(ListItem<LinkEntity> item) {
                    LinkEntity link = item.getModelObject();
                    item.add(new Label("_name", link.getName()));
                    item.add(new Label("_partition", link.getPartition()));
                    item.add(new Label("_readPreference", String.valueOf(link.getReadPreference())));
                    item.add(new Label("_writePreference", String.valueOf(link.getWritePreference())));
                    item.add(new Label("_restorePreference", String.valueOf(link.getRestorePreference())));
                    item.add(new Label("_p2pPreference", String.valueOf(link.getP2pPreference())));
                    String[] unitGroups = extractUnitGroupNames(link.getUnitGroupsFollowed());
                    item.add(new Label("storage", unitGroups[0]));
                    item.add(new Label("net", unitGroups[1]));
                    item.add(new Label("protocol", unitGroups[2]));
                    item.add(new Label("cacheClass", unitGroups[3]));
                    String poolGroups = extractPoolGroupNames(link.getTargetPoolGroups());
                    item.add(new Label("poolGroups", poolGroups));
                    item.add(new Label("pools", ""));
                }

                private String[] extractUnitGroupNames(List<EntityReference> list) {
                    String[] unitGroups = new String[4];
                    int counter = 0;
                    for (EntityReference unitGroup : list) {
// just show the first 4 UnitGroups -- more are not even a sensible configuration of dCache
                        if (counter > 3) {
                            break;
                        }
                        unitGroups[counter] = unitGroup.getName();
                        counter++;
                    }
                    return unitGroups;
                }

                private String extractPoolGroupNames(List<EntityReference> list) {
                    StringBuilder poolGroups = new StringBuilder();
                    boolean commaNeeded = false;
                    for (EntityReference poolGroup : list) {
                        poolGroups.append(poolGroup.getName());
                        if (commaNeeded) {
                            poolGroups.append(",");
                        }
                        commaNeeded = true;
                    }
                    return poolGroups.toString();
                }
            };
            add(linkList);


        }
    }
}
