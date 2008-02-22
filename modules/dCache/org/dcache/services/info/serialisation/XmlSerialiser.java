package org.dcache.services.info.serialisation;

import org.dcache.services.info.base.*;

import java.util.*;

/**
 * This serialiser maps the dCache state directly into an XML InfoSet.
 * 
 * For the most part, this is a simple mapping with some support for handling
 * branch-nodes with a known special parent branch differently.
 * 
 * NB, instances of this Class are not thread-safe: the caller is responsible for
 * ensuring no concurrent calls to serialise().
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class XmlSerialiser implements StateVisitor, StateSerialiser {
	
	
	/** The types used within the XML structure */
	private static final String _newline = "\n";
	
	private static final String _xmlns = "http://www.dcache.org/2008/dCache/state";

	private StringBuffer _out;
	private int _indentationLevel = 0;
	private String _indentationPrefix = ""; 
	private boolean _isTopBranch;

	private StatePath _lastBranchPath;
	private String _lastBranchElementName;
	private String _lastBranchIdName;
	
	
	private class Attribute {
		String name, value;
		Attribute( String iName, String iValue) {
			name = iName;
			value = iValue;
		}
	}
		
	
	/**
	 *  Serialise the current dCache state into XML;
	 *  @return a String containing dCache current state as XML data.
	 */
	public String serialise() {
		return serialise( null);
	}
	
	public String serialise( StatePath start) {
		_out = new StringBuffer();
		_isTopBranch = true;
		_lastBranchPath = null;

		addElement( "<?xml version=\"1.0\"?>");
		
		State.getInstance().visitState( this, start);
		
		return _out.toString();		
	}

	
	public String getName() {
		return "xml";
	}

	/* Deal with branch movement */
	public void visitCompositePreDescend( StatePath path, Map<String,String> metadata)      { enteringBranch( path, metadata); }
	public void visitCompositePreSkipDescend( StatePath path, Map<String,String> metadata)  { enteringBranch( path, metadata); }	
	public void visitCompositePreLastDescend( StatePath path, Map<String,String> metadata)  {}
	public void visitCompositePostDescend( StatePath path, Map<String,String> metadata)     { exitingBranch( path, metadata); }
	public void visitCompositePostSkipDescend( StatePath path, Map<String,String> metadata) { exitingBranch( path, metadata); }
	
	/* Deal with metric values */
	public void visitInteger( StatePath path, IntegerStateValue value) {
		emitLastBranch(false);
		addElement( buildMetricElement( path.getLastElement(), value.getTypeName(), value.toString()));
	}

	public void visitString( StatePath path, StringStateValue value) {
		emitLastBranch(false);
		addElement( buildMetricElement( path.getLastElement(), value.getTypeName(), xmlMarkup(value.toString())));
	}
	
	public void visitBoolean( StatePath path, BooleanStateValue value) {
		emitLastBranch(false);
		addElement( buildMetricElement( path.getLastElement(), value.getTypeName(), value.toString()));
	}
	
	public void visitFloatingPoint( StatePath path, FloatingPointStateValue value) {
		emitLastBranch( false);
		addElement( buildMetricElement( path.getLastElement(), value.getTypeName(), value.toString()));		
	}
	
	/**
	 *  Provide all appropriate activity when entering a new branch.
	 *  <p>
	 *  When dealing with lists, we use the branch metadata:
	 *  <ul>
	 *  <li> METADATA_BRANCH_CLASS_KEY is the name of the list item class (e.g.,
	 *       for items under the dCache.pools branch, this is &quot;pool&quot;)
	 *  <li> METADATA_BRANCH_IDNAME_KEY is the name of identifier (e.g., &quot;name&quot;)
	 *  </ul>  
	 *  <p>
	 *  We mostly push information onto a (single item) stack so we can
	 *  emit empty branches like:
	 *  <pre>
	 *    <branchname attr1="value1" />
	 *  </pre>
	 *  
	 *  @param path The path of the new branch
	 *  @param metadata The keyword-value pairs for this branch. 
	 */
	private void enteringBranch( StatePath path, Map<String,String> metadata) {
		emitLastBranch( false);
		
		String branchClass = null;
		_lastBranchIdName = null;
		
		if( metadata != null) {
			branchClass = metadata.get( State.METADATA_BRANCH_CLASS_KEY);			
			_lastBranchIdName = metadata.get( State.METADATA_BRANCH_IDNAME_KEY);
		}
		
		_lastBranchPath = path;
		_lastBranchElementName = branchClass != null ? branchClass : path.getLastElement();		
	}
	
	/**
	 * 
	 * @param path
	 * @param metadata
	 */
	private void exitingBranch( StatePath path, Map<String,String> metadata) {
		if( path.equals( _lastBranchPath)) {
			emitLastBranch( true);
		} else {
			emitLastBranch( false); // this should be a noop.
			
			_indentationLevel--;
			updateIndentPrefix();
			
			String lastElement = path.getLastElement();
			String branchClass = metadata != null ? metadata.get( State.METADATA_BRANCH_CLASS_KEY) : null;
			
			addElement( endElement( branchClass != null ? branchClass : lastElement));
		}		
	}
	
	
	/**
	 * emit XML for the previous branch.
	 */
	private void emitLastBranch( boolean isEmpty) {

		if( _lastBranchPath == null)
			return;
		
		Attribute[] attrs = null;
		
		if( _isTopBranch) {
			attrs = new Attribute[1];
			attrs[0] = new Attribute( "xmlns", _xmlns);
			_isTopBranch = false;
		} else {
			if( _lastBranchIdName != null) {
				attrs = new Attribute[1];
				attrs[0] = new Attribute( _lastBranchIdName, _lastBranchPath.getLastElement());				
			}
			
		}
		
		addElement( beginElement( _lastBranchElementName, attrs, isEmpty));

		if( !isEmpty) {
			_indentationLevel++;
			updateIndentPrefix();
		}

		_lastBranchPath = null;
	}
	
	
	
	/**
	 * Add an element to the output stream with correct indentation.
	 * @param element the text (element, PI, ...) to add.
	 */
	private void addElement( String element) {
		_out.append( _indentationPrefix);
		_out.append( element);
		_out.append( _newline);
	}
	
	/**
	 * Build an XML metric element based on information.
	 * @param name the name of the metric
	 * @param type the type of the metric
	 * @param value the value
	 */
	private String buildMetricElement( String name, String type, String value) {
		StringBuffer sb = new StringBuffer();
		Attribute attr[] = new Attribute[1];
		attr[0] = new Attribute( "type", type);
		
		sb.append( beginElement( name, attr, false));
		sb.append( value);
		sb.append( endElement(name));

		return sb.toString();
	}
	
		
	
	/**
	 * Build a String that opens an element
	 * @param name the element's name
	 * @param attr either an array of attributes for this element, or null.
	 * @param isEmpty whether the element contains no data.
	 * @return a String representing the start of this element
	 */
	private String beginElement( String name, Attribute[] attr, boolean isEmpty) {
		StringBuffer sb = new StringBuffer();
		
		sb.append("<" + name);
		
		if( attr != null) {			
			for( int i = 0; i < attr.length; i++) {
				sb.append( " ");
				sb.append( attr[i].name);
				sb.append( "=\"");
				sb.append( attr[i].value);
				sb.append( "\"");
			}
		}
		
		if( isEmpty)
			sb.append( "/");
		
		sb.append(">");
		
		return sb.toString();		
	}

	
	/**
	 * Build a string that closes an element
	 * @param name the name of the element to open
	 * @return a String.
	 */
	private String endElement( String name) {
		return "</"+name+">";
	}

	/**
	 * Mark-up an String so it can be included as XML data.
	 * @param value the string value to mark-up
	 * @return value that is safe to include in as an XML text-node.
	 */
	private String xmlMarkup( String value) {
		// TODO: needs an actual implementation.
		return value;
	}

	
	/**
	 * Update our stored prefix for indentation.
	 */
	private void updateIndentPrefix() {
		StringBuffer sb = new StringBuffer();
		
		for( int i = 0; i < _indentationLevel; i++)
			sb.append( "  ");
		
		_indentationPrefix = sb.toString();
	}


}
