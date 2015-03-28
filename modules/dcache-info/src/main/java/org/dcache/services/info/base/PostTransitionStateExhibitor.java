package org.dcache.services.info.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The PostTransitionStateExhibitor provides a StateExhibitor that allows a
 * visitor (a class implementing StateVisitor) to visit what the state will be
 * after a StateTransition has been applied.
 */
public class PostTransitionStateExhibitor implements StateExhibitor
{
    private static final Logger _log =
            LoggerFactory.getLogger(PostTransitionStateExhibitor.class);

    /**
     * The PreTransitionVisitor class visits the current state on behalf of
     * some other StateVisitor. The results are relayed to this other visitor
     * after being adjusted to take into account the StateTransition.
     */
    private class PreTransitionVisitor implements StateVisitor
    {
        private final StateVisitor _postTransitionVisitor;
        private final Set<StatePath> _bannedSubtrees = new HashSet<>();

        public PreTransitionVisitor(StateVisitor postTransitionVisitor)
        {
            _postTransitionVisitor = postTransitionVisitor;
        }

        @Override
        public void visitBoolean(StatePath path, BooleanStateValue value)
        {
            if (isMetricUpdatedOrDeleted(path)) {
                visitUpdatedOrDeletedMetric(path, value);
            } else {
                _postTransitionVisitor.visitBoolean(path, value);
            }
        }

        @Override
        public void visitFloatingPoint(StatePath path, FloatingPointStateValue value)
        {
            if (isMetricUpdatedOrDeleted(path)) {
                visitUpdatedOrDeletedMetric(path, value);
            } else {
                _postTransitionVisitor.visitFloatingPoint(path, value);
            }
        }

        @Override
        public void visitInteger(StatePath path, IntegerStateValue value)
        {
            if (isMetricUpdatedOrDeleted(path)) {
                visitUpdatedOrDeletedMetric(path, value);
            } else {
                _postTransitionVisitor.visitInteger(path, value);
            }
        }

        @Override
        public void visitString(StatePath path, StringStateValue value)
        {
            if (isMetricUpdatedOrDeleted(path)) {
                visitUpdatedOrDeletedMetric(path, value);
            } else {
                _postTransitionVisitor.visitString(path, value);
            }
        }

        private boolean isMetricUpdatedOrDeleted(StatePath metricPath)
        {
            StatePath parentPath = metricPath.parentPath();
            String metricName = metricPath.getLastElement();

            StateChangeSet scs = _transition.getStateChangeSet(parentPath);
            boolean isRemoved = scs != null && scs.childIsRemoved(metricName);
            boolean isUpdated = scs != null && scs.childIsUpdated(metricName);

            return isRemoved || isUpdated || hasBannedParent(metricPath);
        }

        private void visitUpdatedOrDeletedMetric(StatePath path, StateValue value)
        {
            _log.debug("path={}  value={}", path, value);

            StatePath parentPath = path.parentPath();
            String name = path.getLastElement();

            StateChangeSet scs = _transition.getStateChangeSet(parentPath);

            if (scs != null && scs.childIsUpdated(name)) {
                StateComponent updatedComponent =
                        scs.getUpdatedChildValue(name);
                visitUpdatedMetricValue(path, updatedComponent);
            } else {
                // don't visit child as it is to be removed.
            }
        }

        private void visitUpdatedMetricValue(StatePath path, StateComponent component)
        {
            _log.debug("path={}  component={}", path, component);
            if (component instanceof StateComposite) {
                // This is when a metric has become a branch.
                component.acceptVisitor(path, this);
            } else {
                component.acceptVisitor(path, _postTransitionVisitor);
            }
        }

        @Override
        public void visitCompositePreDescend(StatePath path,
                Map<String, String> metadata)
        {
            if (path == null) {
                _postTransitionVisitor
                        .visitCompositePreDescend(null, metadata);
                return;
            }

            StatePath parentPath = path.parentPath();
            String componentName = path.getLastElement();
            StateChangeSet scs = _transition.getStateChangeSet(parentPath);

            if (scs != null && scs.childIsRemoved(componentName)) {
                banChildrenOf(path);
                return;
            }

            if (scs != null && scs.childIsUpdated(componentName)) {
                StateComponent updatedComponent =
                        scs.getUpdatedChildValue(componentName);
                visitUpdatedStateComponentPreDescend(path, updatedComponent,
                            metadata);
            } else {
                _postTransitionVisitor.visitCompositePreDescend(path, metadata);
            }
        }

        private void visitUpdatedStateComponentPreDescend(StatePath path,
                StateComponent updatedComponent, Map<String, String> metadata)
        {
            if (updatedComponent instanceof StateComposite) {
                _postTransitionVisitor.visitCompositePreDescend(path, metadata);
            } else {
                // This is a StateComposite that has become a metric.
                updatedComponent.acceptVisitor(path, _postTransitionVisitor);
                banChildrenOf(path);
            }
        }

        private void banChildrenOf(StatePath parentPath)
        {
            _bannedSubtrees.add(parentPath);
        }

        private boolean hasBannedParent(StatePath path)
        {
            for (StatePath bannedParent : _bannedSubtrees) {
                if (bannedParent.isParentOf(path)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public void visitCompositePostDescend(StatePath path,
                Map<String, String> metadata)
        {
            if (!shouldPostVisitComposite(path)) {
                return;
            }

            visitNewChildren(path);
            _postTransitionVisitor.visitCompositePostDescend(path, metadata);
        }

        private boolean shouldPostVisitComposite(StatePath path)
        {
            if (path == null) {
                return true;
            }

            StatePath parentPath = path.parentPath();
            StateChangeSet parentScs =
                    _transition.getStateChangeSet(parentPath);

            if (parentScs != null) {
                String branchName = path.getLastElement();

                if (parentScs.childIsRemoved(branchName)) {
                    return false;
                }

                if (parentScs.childIsUpdated(branchName)) {
                    StateComponent updatedComponent =
                            parentScs.getUpdatedChildValue(branchName);
                    if (updatedComponent instanceof StateValue) {
                        // This is when a StateComponent has turned into a metric
                        return false;
                    }
                }
            }

            return true;
        }

        private void visitNewChildren(StatePath compositePath)
        {
            StateChangeSet thisBranchScs =
                    _transition.getStateChangeSet(compositePath);

            if (thisBranchScs == null) {
                return;
            }

            for (String newChildName : thisBranchScs.getNewChildren()) {
                StatePath childPath;

                if (compositePath != null) {
                    childPath = compositePath.newChild(newChildName);
                } else {
                    childPath = new StatePath(newChildName);
                }

                StateComponent newComponent =
                        thisBranchScs.getNewChildValue(newChildName);

                visitNewChild(childPath, newComponent);
            }
        }

        private void visitNewChild(StatePath path, StateComponent component)
        {
            _log.debug("Visiting new child: {}", path);

            if (component instanceof StateComposite) {
                component.acceptVisitor(path, this);
            } else {
                component.acceptVisitor(path, _postTransitionVisitor);
            }
        }

        @Override
        public boolean isVisitable(StatePath path)
        {
            if (hasBannedParent(path)) {
                return false;
            }

            return _postTransitionVisitor.isVisitable(path);
        }
    }

    private final StateTransition _transition;
    private final StateExhibitor _currentStateExhibitor;

    public PostTransitionStateExhibitor(StateExhibitor exhibitor,
            StateTransition transition)
    {
        _currentStateExhibitor = exhibitor;
        _transition = transition;
    }

    @Override
    public void visitState(StateVisitor visitor)
    {
        StateVisitor preTransitionVisitor = new PreTransitionVisitor(visitor);
        _currentStateExhibitor.visitState(preTransitionVisitor);
    }
}
