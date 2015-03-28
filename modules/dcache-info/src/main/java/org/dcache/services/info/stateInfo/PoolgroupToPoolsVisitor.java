package org.dcache.services.info.stateInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StateGuide;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateVisitor;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.base.guides.SubtreeStateGuide;

/**
 * Scan through a dCache state tree, building a list of poolgroup-to-pools associations.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PoolgroupToPoolsVisitor implements StateVisitor {

    private static Logger _log = LoggerFactory.getLogger(PoolgroupToPoolsVisitor.class);

    private static final StatePath POOLGROUPS_PATH = new StatePath("poolgroups");

    /**
     * Obtain a Map between a poolgroup and the pools that are currently members of this poolgroup.
     * @return
     */
    public static Map <String,Set<String>> getDetails(StateExhibitor exhibitor) {
        if (_log.isInfoEnabled()) {
            _log.info("Gathering current status");
        }

        PoolgroupToPoolsVisitor visitor = new PoolgroupToPoolsVisitor();
        exhibitor.visitState(visitor);
        return visitor._poolgroups;
    }

    Map <String,Set<String>> _poolgroups = new HashMap<>();
    Set<String> _currentPoolgroupPools;
    StatePath _poolMembershipPath;
    StateGuide _guide = new SubtreeStateGuide(POOLGROUPS_PATH);

    @Override
    public boolean isVisitable(StatePath path) {
        return _guide.isVisitable(path);
    }

    @Override
    public void visitCompositePreDescend(StatePath path, Map<String,String> metadata) {
        if (_log.isDebugEnabled()) {
            _log.debug("Examining " + path);
        }

        // If something like poolgroups.<some poolgroup>
        if (POOLGROUPS_PATH.isParentOf(path)) {
            if (_log.isDebugEnabled()) {
                _log.debug("Found poolgroup " + path.getLastElement());
            }

            _currentPoolgroupPools = new HashSet<>();
            _poolgroups.put(path.getLastElement(), _currentPoolgroupPools);

            _poolMembershipPath = path.newChild("pools");
        }

        // If something like poolgroups.<some poolgroup>.pools.<some pool>
        if (_poolMembershipPath != null && _poolMembershipPath.isParentOf(path)) {
            if (_log.isDebugEnabled()) {
                _log.debug("Found pool " + path.getLastElement());
            }

            _currentPoolgroupPools.add(path.getLastElement());
        }
    }

    @Override
    public void visitCompositePostDescend(StatePath path, Map<String,String> metadata) {
    }

    @Override
    public void visitString(StatePath path, StringStateValue value) {
    }

    @Override
    public void visitBoolean(StatePath path, BooleanStateValue value) {
    }

    @Override
    public void visitInteger(StatePath path, IntegerStateValue value) {
    }

    @Override
    public void visitFloatingPoint(StatePath path, FloatingPointStateValue value) {
    }
}


