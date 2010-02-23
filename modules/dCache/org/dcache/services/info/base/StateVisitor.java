package org.dcache.services.info.base;

import java.util.Map;


/**
 * Interface for an algorithm that wishes to iterate over the current state.
 * Any algorithm that wishes to operate over (some subset of) the dCache state must
 * implement this interface.
 * <p>
 * Subclasses of StateValues will call the corresponding method.
 * <p>
 * StateComposite objects call visitCompositePreDescend() before visiting their children.
 * Before visiting the last child, the StateComposite will call
 * visitCompositePreLastDescend().  Once all children have been visited the StateComposite
 * will call visitCompositePostDescend().  If a StateComposite has no children, then
 * visitCompositePreLastDescend() is <i>not</i> called.
 * <p>
 * For all Composite methods, the metadata parameter is either null or references a
 * keyword-value repository of metadata.  This metadata is specific for the particular
 * StateComposite.  The metadata is persistent and independent of the actual contents
 * of the State tree; it is linked to a nodes path within the state.
 * <p>
 * If State.visitState() is called with a non-null start path, then the visitor will
 * visit only these elements using visitCompositePreSkipDescend(), then visit the
 * state normally, then finally visit the start path using visitCompositePostSkipDescend()
 * <p>
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public interface StateVisitor {

	/** All subclasses of StateValue must be represented here. */
	void visitString( StatePath path, StringStateValue value);
	void visitInteger( StatePath path, IntegerStateValue value);
	void visitBoolean( StatePath path, BooleanStateValue value);
	void visitFloatingPoint( StatePath path, FloatingPointStateValue value);

	/** StateComposites call pre- and post- traversal */
	void visitCompositePreDescend( StatePath path, Map<String,String> metadata);
	void visitCompositePostDescend( StatePath path, Map<String,String> metadata);

	/** StateComposites call these methods when skipping over higher-level state */
	void visitCompositePreSkipDescend( StatePath path, Map<String,String> metadata);
	void visitCompositePostSkipDescend( StatePath path, Map<String,String> metadata);
}
