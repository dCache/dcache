package org.dcache.services.info.serialisation;

import java.util.BitSet;
import java.util.Map;

import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.State;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateValue;
import org.dcache.services.info.base.StateVisitor;
import org.dcache.services.info.base.StringStateValue;


/**
 * Create a pretty-print output of dCache state using ASCII-art.
 * <p>
 * This output has the advantage of making the tree structure more
 * clear (compared to SimpleTextSerialiser) but the disadvantage of
 * taking up more space.
 *
 * @see SimpleTextSerialiser
 */
public class PrettyPrintTextSerialiser implements StateVisitor, StateSerialiser {

	private static final String MARK_STR  = " | ";
	private static final String SPACE_STR = "   ";
	private static final String DCACHE_LABEL = "dCache";

	private StringBuilder _out;
	private String _prefix;
	private BitSet _tails = new BitSet();
	private int _depth;
	private boolean _nextShouldHaveBlankLine;
	private final StateExhibitor _exhibitor;

	public PrettyPrintTextSerialiser( StateExhibitor exhibitor) {
		_exhibitor = exhibitor;
	}

	/**
	 * Our official name.
	 */
	public String getName() {
		return "pretty-print";
	}


	/**
	 * Build out pretty-print output.
	 * <p>
	 * NB.  This method is <i>not</i> thread-safe.
	 */
	public String serialise( StatePath path) {
	    clearState();
		_exhibitor.visitState(this, path);
		return _out.toString();
	}

	/**
	 * Provide serialisation, starting from top-most dCache state.
	 */
	public String serialise() {
        clearState();
        _exhibitor.visitState(this);
        return _out.toString();
	}


	private void clearState() {
	    _out = new StringBuilder();
	    _tails.clear();
	    _prefix = "";
	    _depth = 0;  // Shouldn't be necessary
	    _nextShouldHaveBlankLine = false;
	}



	/* When skipping, do nothing */
	public void visitCompositePreSkipDescend( StatePath path, Map<String, String> metadata) {}
	public void visitCompositePostSkipDescend( StatePath path, Map<String, String> metadata) {}

	public void visitCompositePreDescend(StatePath path,
			Map<String, String> metadata) {

		StringBuilder labelSB = new StringBuilder();

		if( _depth == 0) {
			labelSB.append( DCACHE_LABEL);

			if( path != null) {
				labelSB.append( ".");
				labelSB.append( path);
			}

		} else {
			boolean added = false;

			outputEmpty();

			if( metadata != null) {
				String className = metadata.get(State.METADATA_BRANCH_CLASS_KEY);
				String idName = metadata.get(State.METADATA_BRANCH_IDNAME_KEY);

				if( className != null && idName != null) {
					labelSB.append( className);
					labelSB.append( ", ");
					labelSB.append( idName);
					labelSB.append( "=\"");
					labelSB.append( path.getLastElement());
					labelSB.append( "\"");
					added = true;
				}
			}

			if( !added)
				labelSB.append( path.getLastElement());
		}

		outputBranch( labelSB.toString());

		_tails.set( _depth);
		_depth++;
		updatePrefix();

		_nextShouldHaveBlankLine = true;
	}

	public void visitCompositePreLastDescend(StatePath path,
			Map<String, String> metadata) {
		_tails.clear( _depth-1);
		updatePrefix();
	}

	public void visitCompositePostDescend(StatePath path,
			Map<String, String> metadata) {
		_depth--;
		updatePrefix();

		_nextShouldHaveBlankLine = true;
	}

	public void visitBoolean(StatePath path, BooleanStateValue value)             { outputMetric( path.getLastElement(), value); }
	public void visitFloatingPoint(StatePath path, FloatingPointStateValue value) { outputMetric( path.getLastElement(), value); }
	public void visitInteger(StatePath path, IntegerStateValue value)             { outputMetric( path.getLastElement(), value); }
	public void visitString(StatePath path, StringStateValue value)               { outputMetric( path.getLastElement(), value); }


	/**
	 * Output a generic metric
	 * @param name the name of the metric
	 * @param value the StateValue object representing the metric's value.
	 */
	private void outputMetric( String name, StateValue value) {

		if( _nextShouldHaveBlankLine) {
			_nextShouldHaveBlankLine = false;
			outputEmpty();
		}

		_out.append( _prefix);
		_out.append( " +--");
		_out.append(name);
		_out.append( ": ");
		if( value instanceof StringStateValue)
			_out.append( "\"");
		_out.append( value.toString());
		if( value instanceof StringStateValue)
			_out.append( "\"");
		_out.append( "  [");
		_out.append( value.getTypeName());
		_out.append( "]\n");
	}


	/**
	 * Print a new branch.
	 * @param name the name of the branch.
	 */
	private void outputBranch( String name) {
		_out.append( _prefix);

		if( _depth > 0)
			_out.append( " +-");

		_out.append( "[");
		_out.append( name);
		_out.append("]\n");
	}


	/**
	 * Output an empty line.
	 */
	private void outputEmpty() {
		_out.append( _prefix);
		_out.append( MARK_STR);
		_out.append( "\n");
	}


	/**
	 * Update the prefix, based on changing value of _tails.
	 */
	private void updatePrefix() {
		StringBuilder sb = new StringBuilder();

		for( int i = 0; i < _depth-1; i++)
			sb.append( _tails.get(i) ? MARK_STR : SPACE_STR);

		_prefix = sb.toString();
	}
}
