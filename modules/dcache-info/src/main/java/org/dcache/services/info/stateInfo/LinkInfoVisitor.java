package org.dcache.services.info.stateInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.stateInfo.LinkInfo.UNIT_TYPE;

/**
 * Scan through the known list of links and build up an associated collection
 * of LinkInfo objects.
 */
public class LinkInfoVisitor extends SkeletonListVisitor
{
    private static Logger _log = LoggerFactory.getLogger(LinkInfoVisitor.class);

    private static final StatePath LINK_PATH = StatePath.parsePath("links");

    private static final String LINK_POOLS_PATH_ELEMENT = "pools";
    private static final String LINK_POOLGROUPS_PATH_ELEMENT = "poolgroups";
    private static final String LINK_UNITGROUPS_PATH_ELEMENT = "unitgroups";
    private static final String LINK_UNITS_PATH_ELEMENT = "units";
    private static final String LINK_SPACE_PATH_ELEMENT = "space";

    /*
     * The names of the branches for different collections of units.
     */
    private static final String LINK_STORE_PATH_ELEMENT = "store";
    private static final String LINK_NET_PATH_ELEMENT = "net";
    private static final String LINK_PROTO_PATH_ELEMENT = "protocol";
    private static final String LINK_DCACHE_PATH_ELEMENT = "dcache";

    private static final String LINK_PREFS_PATH_ELEMENT = "prefs";

    /*
     * The names under which metrics for different link operations are
     * stored.
     */
    private static final String LINK_READ_METRIC_PATH_ELEMENT = "read";
    private static final String LINK_WRITE_METRIC_PATH_ELEMENT = "write";
    private static final String LINK_CACHE_METRIC_PATH_ELEMENT = "cache";
    private static final String LINK_P2P_METRIC_PATH_ELEMENT = "p2p";

    /**
     * A simple Map that allows us to convert a string into the corresponding
     * LinkInfo.UNIT_TYPE enum.
     */
    public static final Map<String, LinkInfo.UNIT_TYPE> UNIT_TYPE_NAMES =
            Collections.unmodifiableMap(new HashMap<String, LinkInfo.UNIT_TYPE>() {
                private static final long serialVersionUID =
                        4631442886053020941L;
                {
                    put(LINK_STORE_PATH_ELEMENT, UNIT_TYPE.STORE);
                    put(LINK_NET_PATH_ELEMENT, UNIT_TYPE.NETWORK);
                    put(LINK_DCACHE_PATH_ELEMENT, UNIT_TYPE.DCACHE);
                    put(LINK_PROTO_PATH_ELEMENT, UNIT_TYPE.PROTOCOL);
                }
            });

    /**
     * A simple Map that allows us to convert a String into the corresponding
     * LinkInfo.OPERATION enum.
     */
    public static final Map<String, LinkInfo.OPERATION> OPERATION_NAMES =
            Collections.unmodifiableMap(new HashMap<String, LinkInfo.OPERATION>() {
                private static final long serialVersionUID =
                        8199146124808181726L;
                {
                    put(LINK_READ_METRIC_PATH_ELEMENT, LinkInfo.OPERATION.READ);
                    put(LINK_WRITE_METRIC_PATH_ELEMENT,
                            LinkInfo.OPERATION.WRITE);
                    put(LINK_CACHE_METRIC_PATH_ELEMENT,
                            LinkInfo.OPERATION.CACHE);
                    put(LINK_P2P_METRIC_PATH_ELEMENT, LinkInfo.OPERATION.P2P);
                }
            });

    /**
     * Obtain information about the current links in dCache
     *
     * @return a Mapping between a link's ID and the corresponding LinkInfo.
     */
    public static Map<String, LinkInfo> getDetails(StateExhibitor exhibitor)
    {
        _log.debug("Gathering link information.");

        LinkInfoVisitor visitor = new LinkInfoVisitor();
        exhibitor.visitState(visitor);

        return visitor.getInfo();
    }

    /** The mapping between link names and corresponding LinkInfo object */
    private final Map<String, LinkInfo> _links = new HashMap<>();

    private LinkInfo _thisLink;
    private StatePath _thisLinkPoolsPath;
    private StatePath _thisLinkPoolgroupPath;
    private StatePath _thisLinkUnitgroupsPath;
    private StatePath _thisLinkUnitsPath;
    private StatePath _thisLinkSpacePath;
    private StatePath _thisLinkOperationPrefPath;
    private StatePath _thisLinkPath;

    public LinkInfoVisitor()
    {
        super(LINK_PATH);
    }

    @Override
    protected void newListItem(String listItemName)
    {
        super.newListItem(listItemName);

        _thisLink = new LinkInfo(listItemName);
        _links.put(listItemName, _thisLink);

        /**
         * Build up the various StatePaths where we expect data to appear for
         * this link.
         */
        _thisLinkPath = LINK_PATH.newChild(listItemName);

        _thisLinkPoolsPath = _thisLinkPath.newChild(LINK_POOLS_PATH_ELEMENT);
        _thisLinkPoolgroupPath =
                _thisLinkPath.newChild(LINK_POOLGROUPS_PATH_ELEMENT);
        _thisLinkUnitgroupsPath =
                _thisLinkPath.newChild(LINK_UNITGROUPS_PATH_ELEMENT);
        _thisLinkUnitsPath = _thisLinkPath.newChild(LINK_UNITS_PATH_ELEMENT);
        _thisLinkSpacePath = _thisLinkPath.newChild(LINK_SPACE_PATH_ELEMENT);
        _thisLinkOperationPrefPath =
                _thisLinkPath.newChild(LINK_PREFS_PATH_ELEMENT);
    }

    @Override
    public void visitCompositePreDescend(StatePath path,
            Map<String, String> metadata)
    {
        super.visitCompositePreDescend(path, metadata);

        /** Only process items within subtree starting link.<link id> */
        if (!isInListItem()) {
            return;
        }

        String listItem = path.getLastElement();

        /**
         * Skip if we're looking at link.<link id>.units or link.<link
         * id>.units.<UNIT_TYPE>
         */
        if (_thisLinkUnitsPath.equals(path) ||
                (_thisLinkUnitsPath.isParentOf(path) && UNIT_TYPE_NAMES.containsKey(listItem))) {
            return;
        }

        /*
         * Skip  link.<link id>.space and any child element of this path
         */
        if (_thisLinkSpacePath.equals(path) || _thisLinkSpacePath.isParentOf(path)) {
            return;
        }

        StatePath parentPath = path.parentPath();
        String parentLastElement = parentPath.getLastElement();

        /**
         * If we're looking at the parent element of any lists, ignore this
         * item
         */
        if (_thisLinkPath.equals(path) ||
                _thisLinkOperationPrefPath.equals(path) ||
                _thisLinkUnitsPath.equals(path) ||
                _thisLinkPoolsPath.equals(path) ||
                _thisLinkPoolgroupPath.equals(path) ||
                _thisLinkUnitgroupsPath.equals(path)) {
            return;
        }

        /** If we're looking at link.<link id>.units.<UNIT_TYPE>.<listItem> */
        if (_thisLinkUnitsPath.isParentOf(parentPath) &&
            UNIT_TYPE_NAMES.containsKey(parentLastElement)) {

            if (_log.isDebugEnabled()) {
                _log.debug("Adding pool " + listItem);
            }

            _thisLink.addUnit(UNIT_TYPE_NAMES.get(parentLastElement), listItem);
            return;
        }

        /** If we're looking at link.<link id>.pools.<listItem> */
        if (_thisLinkPoolsPath.isParentOf(path)) {
            if (_log.isDebugEnabled()) {
                _log.debug("Adding pool " + listItem);
            }

            _thisLink.addPool(listItem);
            return;
        }

        /** If we're looking at link.<link id>.poolgroups.<listItem> */
        if (_thisLinkPoolgroupPath.isParentOf(path)) {
            if (_log.isDebugEnabled()) {
                _log.debug("Adding poolgroup " + listItem);
            }

            _thisLink.addPoolgroup(listItem);

            return;
        }

        /** If we're looking at link.<link id>.unitgroups.<listItem> */
        if (_thisLinkUnitgroupsPath.isParentOf(path)) {
            if (_log.isDebugEnabled()) {
                _log.debug("Adding unitgroup " + listItem);
            }

            _thisLink.addUnitgroup(listItem);

            return;
        }

        _log.warn("Unexpected element at " + path);
    }

    /**
     * Called when an integer metric is encountered.
     */
    @Override
    public void visitInteger(StatePath path, IntegerStateValue value)
    {
        if (!isInListItem()) {
            return;
        }

        if (!_thisLinkOperationPrefPath.isParentOf(path)) {
            return;
        }

        String metricName = path.getLastElement();

        if (OPERATION_NAMES.containsKey(metricName)) {
            _thisLink.setOperationPref(OPERATION_NAMES.get(metricName),
                    value.getValue());
        }
    }

    /**
     * Provide the mapping between a link's name and corresponding LinkInfo
     * object.
     *
     * @return
     */
    public Map<String, LinkInfo> getInfo()
    {
        return _links;
    }

    public String debugInfo()
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, LinkInfo> entry : _links.entrySet()) {
            LinkInfo info = entry.getValue();
            sb.append(info.debugInfo());
            sb.append("\n");
        }

        return sb.toString();
    }
}
