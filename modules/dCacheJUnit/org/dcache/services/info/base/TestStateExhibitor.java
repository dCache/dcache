package org.dcache.services.info.base;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

/**
 * The TestStateExhibitor is a light-weight implementation of the
 * StateExhibitor interface. It can be preprogrammed to mimic a realistic
 * dCache state. Classes that implement the StateVisitor interface may visit
 * this state, allowing unit testing.
 * <p>
 * The class has support for simulating the effect of visiting dCache state with a
 * StateTransition and for a non-null skip value.
 * <p>
 * When using a StateTransition object, either when visiting or when updating
 * the metrics it is possible that the StateTransition contains an invalid
 * transition. If such an inconsistency is detected then
 * {@link org.junit.Assert#fail} is called with an appropriate error message.
 */
public class TestStateExhibitor implements StateExhibitor, Cloneable {

    /**
     * Information about a specific Node (either a branch or a metric value)
     */
    private static class Node {
        private final Map<String, Node> _children = new HashMap<String, Node>();
        private final Map<String, String> _metadata = new HashMap<String, String>();
        private final StateValue _metricValue;

        public Node() {
            _metricValue = null;
        }

        public Node( StateValue metric) {
            _metricValue = metric;
        }

        public boolean isMetric() {
            return _metricValue != null;
        }

        /**
         * Obtain a child Node with corresponding name. If Node doesn't exist
         * it is created. If metric is null, any created node will be a
         * branch, otherwise it will be a metric node.
         *
         * @param childName
         * @param metric
         * @return
         */
        private Node getOrCreateChild( String childName, StateValue metric) {
            Node child;
            if( _children.containsKey( childName))
                child = _children.get( childName);
            else {
                if( metric != null)
                    child = new Node( metric);
                else
                    child = new Node();
                _children.put( childName, child);
            }

            return child;
        }

        /**
         * Add a metric at the specific StatePath
         *
         * @param path
         * @param metric
         */
        public void addMetric( StatePath path, StateValue metric) {
            String childName = path.getFirstElement();
            Node child = getOrCreateChild( childName, path.isSimplePath()
                    ? metric : null);

            if( !path.isSimplePath())
                child.addMetric( path.childPath(), metric);
        }

        /**
         * Ensure we have a branch at specific StatePath.
         *
         * @param path
         */
        public void addBranch( StatePath path) {
            String childName = path.getFirstElement();
            Node child = getOrCreateChild( childName, null);
            if( !path.isSimplePath())
                child.addBranch( path.childPath());
        }

        public void addListItem( StatePath path, String type, String idName) {
            String childName = path.getFirstElement();
            Node child = getOrCreateChild( childName, null);

            if( !path.isSimplePath()) {
                child.addListItem( path.childPath(), type, idName);
            } else {
                child._metadata.put( State.METADATA_BRANCH_CLASS_KEY, type);
                child._metadata.put( State.METADATA_BRANCH_IDNAME_KEY, idName);
            }
        }

        /**
         * Support skipping when the current node might be skipped. If the
         * skipPath is null then visit() method is called.
         *
         * @param visitor
         * @param ourPath
         * @param skipPath
         */
        public void visitWithSkip( StateVisitor visitor, StatePath ourPath,
                                   StatePath skipPath) {
            if( skipPath == null) {
                visit( visitor, ourPath);
                return;
            }

            String childName = skipPath.getFirstElement();

            if( _children.containsKey( childName)) {
                Map<String,String> visitMetadata = _metadata.isEmpty() ? null : _metadata;

                visitor.visitCompositePreSkipDescend( ourPath, visitMetadata);

                Node childNode = _children.get( childName);
                StatePath childPath = ourPath == null
                        ? new StatePath( childName)
                        : ourPath.newChild( childName);
                childNode.visitWithSkip( visitor, childPath,
                                         skipPath.childPath());

                visitor.visitCompositePostSkipDescend( ourPath, visitMetadata);
            }
        }

        /**
         * Visit the current state. This simulates visiting a real dCache
         * state.
         *
         * @param visitor
         * @param ourPath
         */
        public void visit( StateVisitor visitor, StatePath ourPath) {
            if( isMetric()) {
                _metricValue.acceptVisitor( ourPath, visitor);
                return;
            }

            Map<String,String> visitMetadata = _metadata.isEmpty() ? null : _metadata;

            visitor.visitCompositePreDescend( ourPath, visitMetadata);

            int counter = 0;
            for( Map.Entry<String, Node> entry : _children.entrySet()) {
                String childName = entry.getKey();
                Node child = entry.getValue();

                if( counter++ == _children.size() - 1)
                    visitor.visitCompositePreLastDescend( ourPath, visitMetadata);

                StatePath childPath = (ourPath == null)
                        ? new StatePath( childName)
                        : ourPath.newChild( childName);

                child.visit( visitor, childPath);
            }

            visitor.visitCompositePostDescend( ourPath, visitMetadata);
        }

        /**
         * Apply a StateTransition to a tree of Nodes. This is does
         * iteratively.
         *
         * @param transition
         * @param myPath
         */
        public void applyTransition( StateTransition transition,
                                     StatePath myPath) {
            StateChangeSet scs = transition.getStateChangeSet( myPath);

            if( scs == null)
                return;

            // Add new children.
            for( String newChildName : scs.getNewChildren()) {
                if( _children.containsKey( newChildName))
                    fail( "Attempt to add new child " + newChildName +
                          " when existing child already exists");

                StateComponent newChildValue = scs.getNewChildValue( newChildName);
                _children.put( newChildName,
                               newNodeFromStateComponent( newChildValue));
            }

            // Update value of children.
            for( String updatedChildName : scs.getUpdatedChildren()) {
                if( !_children.containsKey( updatedChildName))
                    fail( "Attempt to update child " + updatedChildName +
                          " when existing child doesn't exist");

                StateComponent updatedChildValue = scs.getUpdatedChildValue( updatedChildName);
                _children.put( updatedChildName,
                               newNodeFromStateComponent( updatedChildValue));
            }

            // Remove children.
            for( String removedChildName : scs.getRemovedChildren()) {
                if( !_children.containsKey( removedChildName))
                    fail( "Attempt to remove a child " + removedChildName +
                          " when child doesn't exist");

                _children.remove( removedChildName);
            }

            // Iterate down into those children
            for( String itrChildName : scs.getItrChildren())
                if( _children.containsKey( itrChildName)) {
                    StatePath childPath = myPath == null
                            ? new StatePath( itrChildName)
                            : myPath.newChild( itrChildName);
                    _children.get( itrChildName).applyTransition( transition,
                                                                  childPath);
                } else {
                    fail( "Attempt to iterate down into non-existing child");
                }
        }

        /**
         * Create a new Node that is based on a given StateComponent. If the
         * StateComponent is a metric (i.e., a subclass of StateValue) then
         * the new Node will be created with that StateValue. If not, then a
         * branch Node (simulating StateComposite) is created.
         *
         * @param newValue
         * @return
         */
        private static Node newNodeFromStateComponent( StateComponent newValue) {
            Node newNode;

            if( newValue instanceof StateValue) {
                newNode = new Node( (StateValue) newValue);
            } else {
                newNode = new Node();
            }

            return newNode;
        }

        @Override
        public Object clone() {
            Node newNode;

            /**
             * For branch Node objects (corresponding to a StateComposite
             * object) we clone each of the children and update the cloned
             * Node.
             */
            if( !isMetric()) {
                newNode = new Node();

                for( Map.Entry<String, Node> entry : _children.entrySet()) {
                    String childName = entry.getKey();
                    Node thisNodeChildNode = entry.getValue();
                    Node cloneNodeChildNode = (Node) thisNodeChildNode.clone();
                    newNode._children.put( childName, cloneNodeChildNode);
                }
            } else {
                /**
                 * NB. we don't bother to clone the StateValue since
                 * subclasses of StateValue are immutable.
                 */
                newNode = new Node( _metricValue);
            }

            return newNode;
        }
    }

    Node _rootNode = new Node();

    public void addMetric( StatePath path, StateValue metric) {
        _rootNode.addMetric( path, metric);
    }

    public void addBranch( StatePath path) {
        _rootNode.addBranch( path);
    }

    public void addListItem( StatePath path, String type, String idName) {
        _rootNode.addListItem( path, type, idName);
    }

    @Override
    public void visitState( StateVisitor visitor) {
        _rootNode.visit( visitor, null);
    }

    /**
     * This is a special version of the {@link #visitState(StateVisitor)}
     * method. It exists to allow easy testing of the clone method.
     *
     * @param visitor
     */
    public void visitClonedState( StateVisitor visitor) {
        Node newRoot = (Node) _rootNode.clone();
        newRoot.visit( visitor, null);
    }

    @Override
    public void visitState( StateVisitor visitor, StateTransition transition) {
        Node newRoot = (Node) _rootNode.clone();
        newRoot.applyTransition( transition, null);
        newRoot.visit( visitor, null);
    }

    @Override
    public void visitState( StateVisitor visitor, StatePath start) {
        _rootNode.visitWithSkip( visitor, null, start);
    }

    @Override
    public void visitState( StateTransition transition, StateVisitor visitor,
                            StatePath start) {
        Node newRoot = (Node) _rootNode.clone();
        newRoot.applyTransition( transition, null);
        newRoot.visitWithSkip( visitor, null, start);
    }

}
