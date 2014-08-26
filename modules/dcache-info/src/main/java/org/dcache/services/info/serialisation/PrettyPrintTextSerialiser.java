package org.dcache.services.info.serialisation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.State;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateValue;
import org.dcache.services.info.base.StringStateValue;

/**
 * Create a pretty-print output of dCache state using ASCII-art.
 * <p>
 * This output has the advantage of making the tree structure more clear
 * (compared to SimpleTextSerialiser) but the disadvantage of taking up more
 * space.
 *
 * @see SimpleTextSerialiser
 */
public class PrettyPrintTextSerialiser extends SubtreeVisitor implements StateSerialiser {

    private static final String ROOT_ELEMENT_LABEL = "dCache";

    public static final String NAME = "pretty-print";

    private StateExhibitor _exhibitor;
    private final Stack<Chunk> _lastChunkStack = new Stack<>();
    private final List<Chunk> _pendingChunks = new ArrayList<>();

    private StatePath _topMostElement;
    private StringBuilder _out;
    private boolean _foundSomething;
    private boolean _nextChunkHasStalk;

    public void setStateExhibitor(StateExhibitor exhibitor)
    {
        _exhibitor = exhibitor;
    }

    /**
     * Our official name.
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Build out pretty-print output.
     * <p>
     * NB. This method is <i>not</i> thread-safe.
     */
    @Override
    public String serialise( StatePath path) {
        clearState();
        _topMostElement = path;
        setVisitScopeToSubtree( path);

        _exhibitor.visitState( this);

        String output = "";
        if( _foundSomething) {
            String header = buildHeader(path.toString());
            _out.append(header).append("\n");
            flushPendingChunks();
            output = _out.toString();
        }

        return output;
    }

    /**
     * Provide serialisation, starting from top-most dCache state.
     */
    @Override
    public String serialise() {
        clearState();
        _topMostElement = null;
        setVisitScopeToEverything();

        _exhibitor.visitState( this);

        String header = buildHeader(null);
        _out.append(header).append("\n");
        flushPendingChunks();
        return _out.toString();
    }

    public String buildHeader( String path) {
        StringBuilder sb = new StringBuilder();
        sb.append( "[" + ROOT_ELEMENT_LABEL);
        if( path != null) {
            sb.append(".").append(path);
        }
        sb.append( "]");
        return sb.toString();
    }

    private void flushPendingChunks() {
        for( Chunk chunk : _pendingChunks) {
            String chunkOutput = chunk.getOutput();
            _out.append( chunkOutput);
        }
    }

    private void clearState() {
        _out = new StringBuilder();
        _pendingChunks.clear();
        _foundSomething = false;
        _nextChunkHasStalk = true;
        _lastChunkStack.clear();
        _lastChunkStack.add( new Chunk());
    }

    private boolean arePathsSame( StatePath p1, StatePath p2) {
        if( p1 == null && p2 == null) {
            return true;
        }
        return (p1 == null) ? false : p1.equals( p2);
    }

    @Override
    public void visitCompositePreDescend( StatePath path,
                                          Map<String, String> metadata) {

        if( ! isInsideScope( path)) {
            return;
        }

        _foundSomething = true;

        if( arePathsSame( path, _topMostElement)) {
            return;
        }

        addBranchChunk( path.getLastElement(), metadata);
        descend();
        _nextChunkHasStalk = true;
    }

    private void addBranchChunk( String name, Map<String, String> metadata) {
        String type = null;
        String idName = null;

        if( metadata != null) {
            type = metadata.get( State.METADATA_BRANCH_CLASS_KEY);
            idName = metadata.get( State.METADATA_BRANCH_IDNAME_KEY);
        }

        EndOfChunkItem item;

        if( type != null && idName != null) {
            item = new ListItem( _nextChunkHasStalk, type, idName, name);
        } else {
            item = new BranchItem( _nextChunkHasStalk, name);
        }

        addSiblingChunk( item);
    }

    private Chunk getThisBranchLastChunk() {
        return _lastChunkStack.lastElement();
    }

    private void setThisBranchLastChunk( Chunk chunk) {
        int lastItemIndex = _lastChunkStack.size() - 1;
        _lastChunkStack.set( lastItemIndex, chunk);
    }

    private void addSiblingChunk( EndOfChunkItem item) {
        Chunk siblingChunk = getThisBranchLastChunk();

        Chunk thisChunk = siblingChunk.newSiblingChunk( item);

        _pendingChunks.add( thisChunk);

        setThisBranchLastChunk( thisChunk);
    }

    private void descend() {
        Chunk siblingChunk = getThisBranchLastChunk();
        _lastChunkStack.add( siblingChunk.newPhantomChildChunk());
    }

    @Override
    public void visitCompositePostDescend( StatePath path,
                                           Map<String, String> metadata) {
        if( ! isInsideScope( path)) {
            return;
        }

        Chunk lastChunk = getThisBranchLastChunk();
        lastChunk.setEndOfList();
        ascend();
    }

    private void ascend() {
        _lastChunkStack.pop();
        _nextChunkHasStalk = true;
    }

    @Override
    public void visitBoolean( StatePath path, BooleanStateValue value) {
        addMetricChunk( path, value);
    }

    @Override
    public void visitFloatingPoint( StatePath path,
                                    FloatingPointStateValue value) {
        addMetricChunk( path, value);
    }

    @Override
    public void visitInteger( StatePath path, IntegerStateValue value) {
        addMetricChunk( path, value);
    }

    @Override
    public void visitString( StatePath path, StringStateValue value) {
        addMetricChunk( path, value);
    }

    private void addMetricChunk( StatePath path, StateValue value) {
        String name = path.getLastElement();
        EndOfChunkItem item = new MetricItem( _nextChunkHasStalk, name, value);
        addSiblingChunk( item);
        _nextChunkHasStalk = false;
    }

    /**
     * This class represents one or two horizontal lines of output. It may
     * represent some dCache item (a metric or branch), or is a phantom.
     * Phantoms take up no vertical space but introduce a new Stem.
     */
    private static class Chunk {
        private static final EndOfChunkItem END_ITEM_FOR_PHANTOM_CHUNK = null;

        private final List<Stem> _stems = new ArrayList<>();
        private final EndOfChunkItem _endItem;
        private Stem _stemForChild;

        @SuppressWarnings("unchecked")
        public Chunk() {
            this( END_ITEM_FOR_PHANTOM_CHUNK, Collections.<Stem>emptyList());
        }

        public Chunk( List<Stem> stems) {
            this( END_ITEM_FOR_PHANTOM_CHUNK, stems);
        }

        public Chunk( EndOfChunkItem item, List<Stem> stems) {
            _endItem = item;
            _stems.addAll( stems);
        }

        public boolean isPhantom() {
            return _endItem == END_ITEM_FOR_PHANTOM_CHUNK;
        }

        private Stem addNewEndStem() {
            Stem newStem = new Stem();
            _stems.add( newStem);
            return newStem;
        }

        public String getOutput() {
            return isPhantom() ? "" : getNonPhantomOutput();
        }

        private String getNonPhantomOutput() {
            StringBuilder sb = new StringBuilder();

            String stemsPrefix = buildStemsPrefix();

            for( String endItemLine : _endItem.getOutput()) {
                sb.append( stemsPrefix);
                sb.append( endItemLine);
            }

            return sb.toString();
        }

        private String buildStemsPrefix() {
            StringBuilder sb = new StringBuilder();

            for( Stem stem : _stems) {
                sb.append(stem.getOutput());
            }

            return sb.toString();
        }

        public Chunk newSiblingChunk( EndOfChunkItem item) {
            return new Chunk( item, _stems);
        }

        public Chunk newPhantomChildChunk() {
            Chunk childChunk = new Chunk( _stems);
            _stemForChild = childChunk.addNewEndStem();
            return childChunk;
        }

        public void setEndOfList() {
            if( _stemForChild != null) {
                _stemForChild.setInvisable();
            }
        }
    }

    /** The Stem class represents the potential to draw a vertical line */
    private static class Stem {
        public static final String STEM_ITEM = " | ";
        public static final String BLANK_ITEM = "   ";

        private boolean _visable = true;

        public void setInvisable() {
            _visable = false;
        }

        public String getOutput() {
            return _visable ? STEM_ITEM : BLANK_ITEM;
        }
    }

    /**
     * Objects of this class represents something to be displayed at the
     * right-hand end of a chunk past the Stems, if any. An EndOfChunkItem
     * may have a "stalk": this introduces a one-line spacer between this
     * chunk and the previous chunk, where the end-item is displayed with a
     * vertical line joining it to the previous end-item.
     */
    private static abstract class EndOfChunkItem {
        private final boolean _hasStalk;

        public EndOfChunkItem( boolean hasStalk) {
            _hasStalk = hasStalk;
        }

        public List<String> getOutput() {
            List<String> output = new ArrayList<>();

            if( _hasStalk) {
                output.add(Stem.STEM_ITEM + "\n");
            }

            output.add( " +-" + getItemLabel() + "\n");

            return output;
        }

        abstract String getItemLabel();
    }

    /** Represent a metric as an EndOfChunkItem */
    private static class MetricItem extends EndOfChunkItem {
        private final String _label;

        public MetricItem( boolean hasStalk, String name, StateValue metric) {
            super( hasStalk);

            String value = getValueOfMetric( metric);
            String type = metric.getTypeName();

            _label = "-" + name + ": " + value + "  [" + type + "]";
        }

        private String getValueOfMetric( StateValue metricValue) {
            String value;

            if( metricValue instanceof StringStateValue) {
                value = "\"" + metricValue.toString() + "\"";
            } else {
                value = metricValue.toString();
            }

            return value;
        }

        @Override
        String getItemLabel() {
            return _label;
        }
    }

    /** Represent a branch as an EndOfChunkItem */
    private static class BranchItem extends EndOfChunkItem {
        private final String _label;

        public BranchItem( boolean hasStalk, String name) {
            super( hasStalk);
            _label = "[" + name + "]";
        }

        @Override
        String getItemLabel() {
            return _label;
        }
    }

    /** Represent a list-item as an EndOfChunkItem */
    private static class ListItem extends EndOfChunkItem {
        private final String _label;

        public ListItem( boolean hasStalk, String type, String idName,
                         String name) {
            super( hasStalk);
            _label = "[" + type + ", " + idName + "=\"" + name + "\"]";
        }

        @Override
        String getItemLabel() {
            return _label;
        }
    }
}
