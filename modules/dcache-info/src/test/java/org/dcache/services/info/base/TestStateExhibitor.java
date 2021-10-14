package org.dcache.services.info.base;

import com.google.common.collect.Ordering;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;

/**
 * The TestStateExhibitor is a light-weight implementation of the StateExhibitor interface. It can
 * be preprogrammed to mimic a realistic dCache state. Classes that implement the StateVisitor
 * interface may visit this state, allowing unit testing.
 * <p>
 * When using a StateTransition object, either when visiting or when updating the metrics it is
 * possible that the StateTransition contains an invalid transition. If such an inconsistency is
 * detected then {@link Assert#fail} is called with an appropriate error message.
 */
public class TestStateExhibitor implements StateExhibitor, Cloneable {

    /**
     * Information about a specific Node (either a branch or a metric value)
     */
    private static class Node {

        private final Map<String, Node> _children = new HashMap<>();
        private final Map<String, String> _metadata = new HashMap<>();
        private final StateValue _metricValue;

        public Node() {
            _metricValue = null;
        }

        public Node(StateValue metric) {
            _metricValue = metric;
        }

        public boolean isMetric() {
            return _metricValue != null;
        }

        /**
         * Obtain a child Node with corresponding name. If Node doesn't exist it is created. If
         * metric is null, any created node will be a branch, otherwise it will be a metric node.
         *
         * @param childName
         * @param metric
         * @return
         */
        private Node getOrCreateChild(String childName, StateValue metric) {
            Node child;
            if (_children.containsKey(childName)) {
                child = _children.get(childName);
            } else {
                if (metric != null) {
                    child = new Node(metric);
                } else {
                    child = new Node();
                }
                _children.put(childName, child);
            }

            return child;
        }

        /**
         * Add a metric at the specific StatePath
         *
         * @param path
         * @param metric
         */
        public void addMetric(StatePath path, StateValue metric) {
            String childName = path.getFirstElement();
            Node child = getOrCreateChild(childName, path.isSimplePath()
                  ? metric : null);

            if (!path.isSimplePath()) {
                child.addMetric(path.childPath(), metric);
            }
        }

        /**
         * Ensure we have a branch at specific StatePath.
         *
         * @param path
         */
        public void addBranch(StatePath path) {
            String childName = path.getFirstElement();
            Node child = getOrCreateChild(childName, null);
            if (!path.isSimplePath()) {
                child.addBranch(path.childPath());
            }
        }

        public void addListItem(StatePath path, String type, String idName) {
            String childName = path.getFirstElement();
            Node child = getOrCreateChild(childName, null);

            if (!path.isSimplePath()) {
                child.addListItem(path.childPath(), type, idName);
            } else {
                child._metadata.put(State.METADATA_BRANCH_CLASS_KEY, type);
                child._metadata.put(State.METADATA_BRANCH_IDNAME_KEY, idName);
            }
        }

        /**
         * Visit the current state. This simulates visiting a real dCache state.
         *
         * @param visitor
         * @param ourPath
         */
        public void visit(StateVisitor visitor, StatePath ourPath) {
            if (isMetric()) {
                _metricValue.acceptVisitor(ourPath, visitor);
                return;
            }

            if (!visitor.isVisitable(ourPath)) {
                return;
            }

            Map<String, String> visitMetadata = _metadata.isEmpty() ? null : _metadata;

            visitor.visitCompositePreDescend(ourPath, visitMetadata);

            for (String childName : Ordering.natural().sortedCopy(_children.keySet())) {
                Node child = _children.get(childName);

                StatePath childPath = (ourPath == null)
                      ? new StatePath(childName)
                      : ourPath.newChild(childName);

                child.visit(visitor, childPath);
            }

            visitor.visitCompositePostDescend(ourPath, visitMetadata);
        }
    }

    private final Node _rootNode = new Node();

    public void addMetric(StatePath path, StateValue metric) {
        _rootNode.addMetric(path, metric);
    }

    public void addBranch(StatePath path) {
        _rootNode.addBranch(path);
    }

    public void addListItem(StatePath path, String type, String idName) {
        _rootNode.addListItem(path, type, idName);
    }

    @Override
    public void visitState(StateVisitor visitor) {
        _rootNode.visit(visitor, null);
    }
}
