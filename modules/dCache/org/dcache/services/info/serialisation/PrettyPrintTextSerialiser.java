package org.dcache.services.info.serialisation;

import java.util.*;

import org.dcache.services.info.base.*;

public class PrettyPrintTextSerialiser implements StateVisitor, StateSerialiser {
	
	private static final String MARK_STR  = " | ";
	private static final String SPACE_STR = "   ";
	private static final String DCACHE_LABEL = "dCache";

	private StringBuffer _out;
	private String _prefix;
	private BitSet _tails;
	private int _depth;
	private boolean _nextShouldHaveBlankLine;

	/**
	 * Our official name.
	 */
	public String getName() {
		return "pretty-print";
	}


	/**
	 * Build out pretty-print output.
	 */
	public String serialise( StatePath path) {
		_out = new StringBuffer();
		_tails = new BitSet();
		_depth = 0;
		_prefix = "";
		_nextShouldHaveBlankLine = false;
		
		State.getInstance().visitState(this, path);

		return _out.toString();
	}
	
	public String serialise() {
		return serialise( null);
	}

	/* When skipping, do nothing */
	public void visitCompositePreSkipDescend( StatePath path, Map<String, String> metadata) {}
	public void visitCompositePostSkipDescend( StatePath path, Map<String, String> metadata) {}
	
	public void visitCompositePreDescend(StatePath path,
			Map<String, String> metadata) {
		
		String label;
		
		if( _depth == 0) {
			if( path == null)
				label = DCACHE_LABEL;
			else
				label = DCACHE_LABEL + "." + path.toString();
		} else {

			outputEmpty();

			label = path.getLastElement();			

			if( metadata != null) {
				String className = metadata.get(State.METADATA_BRANCH_CLASS_KEY);
				String idName = metadata.get(State.METADATA_BRANCH_IDNAME_KEY);

				if( className != null && idName != null)
					label = className + ", "+idName+"=\""+path.getLastElement()+"\""; 
			}
		}

		outputBranch( label);

		
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
		StringBuffer sb = new StringBuffer();
		
		for( int i = 0; i < _depth-1; i++)
			sb.append( _tails.get(i) ? MARK_STR : SPACE_STR);

		_prefix = sb.toString();
	}	
}
