package org.dcache.services.info.base;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * The VerifyingVisitor allows the verification that the visitor infrastructure is
 * working as expected.
 *
 * Since the order sibling StateComponents are encountered is not guaranteed, we
 * must describe the structure we expect to encounter rather than simply serialise
 * the output (e.g., as a long String).
 */
public class VerifyingVisitor implements StateVisitor {


	/**
	 * An Exception that indicates the operation has violated the expected behaviour of
	 * the visitor.
	 */
	private static class UnexpectedVisitDataException extends Exception {
		private static final long serialVersionUID = -5609901746889782083L;

		UnexpectedVisitDataException( String reason) {
			super( reason);
		}
	}

	/**
	 * A ComponentInfo holds all information about either a branch (StateComposite)
	 * or a metric (any subclass of StateValue).
	 */
	static class ComponentInfo {
		Map<String,ComponentInfo> _children = new HashMap<String,ComponentInfo>();

		/** Have we visited this metric? (only used for metrics) */
		private boolean _haveSeen;

		/** Which PreDescends we have seen?: only used when this._type == BRANCH */
		private final Set<String> _preDescend = new HashSet<String>();

		/** Which PostDescends we have seen?: only used when this._type == BRANCH */
		private final Set<String> _postDescend = new HashSet<String>();

		/** Which PreSkipDescend we have seen?: only used when this._type == BRANCH */
		private String _preSkipDescend = null;

		/** Which PostSkipDescend we have seen?: only used when this._type == BRANCH */
		private String _postSkipDescend = null;

		/** Number of children that have _type StateComponentType.BRANCH */
		private int _branchChildren;

		/** Visited children */
		private int _visitedChildren;

		public static enum StateComponentType {
			BRANCH( "StateComposite"), STRING( "StringStateValue"), INTEGER("IntegerStateValue"), BOOLEAN("BooleanStateValue"), FLOATINGPOINT("FloatingPointStateValue");

			private final Class<?> _stateComponent;

			StateComponentType( String stateComponentName) {
				String fullName ="org.dcache.services.info.base."+stateComponentName;
				Class<?> component = null;

				try {
					component = Class.forName( fullName);
				} catch (ClassNotFoundException e) {
					System.out.println( "Class " + fullName + " not found");
					component = null;
				} finally {
					_stateComponent = component;
				}
			}


			/**
			 * Identify which enum value is appropriate for a given StateComponent.
			 * @param component
			 * @return
			 * @throws IllegalArgumentException if the component isn't one of the known types.
			 */
			static StateComponentType identify( StateComponent component) {
				for( StateComponentType type : StateComponentType.values()) {
					if( type._stateComponent != null &&
							type._stateComponent.isInstance( component))
						return type;
				}
				throw new IllegalArgumentException( "Unable to identify argument");
			}
		}

		private final String _value;

		protected final StateComponentType _type;

		/**
		 * Create a new BRANCH ComponentInfo
		 */
		protected ComponentInfo() {
			_type = StateComponentType.BRANCH;
			_value = null;
		}

		/**
		 * Create a new ComponentInfo to store a metric.
		 * @param type
		 * @param value
		 */
		private ComponentInfo( StateComponentType type, String value) {
			assert( type != StateComponentType.BRANCH);
			_type = type;
			_value = value;
		}


		/**
		 * Get the branch with the given name.  If it doesn't exist, create it.
		 * @param name
		 * @return
		 */
		ComponentInfo getOrCreateBranch( String name) {
			ComponentInfo info;

			try {
				info = getChild( name);
				assert( info._type == StateComponentType.BRANCH);
			} catch( UnexpectedVisitDataException e) {
				info = new ComponentInfo();
				_children.put( name, info);
				_branchChildren++;
			}

			return info;
		}

		/**
		 * Try to obtain the ComponentInfo of a child element.
		 * @param name the name the child
		 * @return the ComponentInfo corresponding to the child element
		 * @throws UnexpectedVisitDataException if no child element exists with name
		 */
		ComponentInfo getChild( String name) throws UnexpectedVisitDataException {
			if( !_children.containsKey( name))
				throw new UnexpectedVisitDataException( "no child " + name);

			return _children.get( name);
		}


		/**
		 * Add an expected StringStateValue metric as a child of this branch
		 * @param name
		 * @param value
		 */
		void addMetric( String metricName, StateComponentType metricType, String metricValue) {
			assert( _type == StateComponentType.BRANCH);
			_children.put( metricName, new ComponentInfo( metricType, metricValue));
		}

		/**
		 * Mark a metric (immediate child of this branch) as found.
		 * @param path path to metric.
		 * @param metricType actual type of metric
		 * @param metricValue actual value of metric
		 * @throws BadPathException if no metric exists at this path
		 * @throws BadMetricException if there is a problem with metric at this point.
		 */
		void markMetric( String metricName, StateComponentType metricType, String metricValue) throws UnexpectedVisitDataException {

			if( !_children.containsKey( metricName))
				throw new UnexpectedVisitDataException( "received unexpected metric: " + metricName);

			ComponentInfo metricInfo = _children.get( metricName);

			if( metricInfo._type != metricType)
				throw new UnexpectedVisitDataException( "expected type " + metricInfo._type.toString() + ", got " + metricType.toString());

			if( !metricInfo._value.equals( metricValue))
				throw new UnexpectedVisitDataException( "expected value " + metricInfo._value + ", got " + metricValue);

			if( metricInfo._haveSeen == true)
				throw new UnexpectedVisitDataException( "already seen this metric");

			_visitedChildren++;
			metricInfo._haveSeen = true;
		}

		/**
		 * Mark a PreDescend
		 * @param childName
		 */
		void markPreDescend( String childName) throws UnexpectedVisitDataException {
			if( !_children.containsKey( childName))
				throw new UnexpectedVisitDataException( "preDescend for unknown " + childName);

			if( _type != StateComponentType.BRANCH)
				throw new UnexpectedVisitDataException( "preDescend for non-branch " + childName);

			if( _preDescend.size() >= _branchChildren)
				throw new UnexpectedVisitDataException( "preDescend for " + childName + " when " + Integer.toString( _preDescend.size()) + " (of " + Integer.toString( _children.size()) + ") have been seen");

			if( _preDescend.contains( childName))
				throw new UnexpectedVisitDataException( "duplicate preDescend for " + childName);

			_visitedChildren++;
			_preDescend.add( childName);
		}

		void markPostDescend( String childName) throws UnexpectedVisitDataException {
			if( _type != StateComponentType.BRANCH)
				throw new UnexpectedVisitDataException( "postDescend for non-branch "+ childName);

			if( !_children.containsKey( childName))
				throw new UnexpectedVisitDataException( "postDescend for unknown " + childName);

			if( _postDescend.size() >= _branchChildren)
				throw new UnexpectedVisitDataException( "postDescend for " + childName + " when all children have registered");

			if( _postDescend.contains( childName))
				throw new UnexpectedVisitDataException( "duplicate postDescend for " + childName);

			_postDescend.add( childName);
		}

		void markPreSkipDescend( String childName) throws UnexpectedVisitDataException {
			if( _type != StateComponentType.BRANCH)
				throw new UnexpectedVisitDataException( "preSkipDescend for non-branch "+ childName);

			if( !_children.containsKey( childName))
				throw new UnexpectedVisitDataException( "preSkipDescend for unknown " + childName);

			if( _preSkipDescend != null)
				throw new UnexpectedVisitDataException( "preSkipDescend for " + childName + " when already have preSkip " + _preSkipDescend);

			_visitedChildren++;
			_preSkipDescend = childName;
		}

		void markPostSkipDescend( String childName) throws UnexpectedVisitDataException {
			if( _type != StateComponentType.BRANCH)
				throw new UnexpectedVisitDataException( "postSkipDescend for non-branch "+ childName);

			if( !_children.containsKey( childName))
				throw new UnexpectedVisitDataException( "postSkipDescend for unknown " + childName);

			if( _postSkipDescend != null)
				throw new UnexpectedVisitDataException( "postSkipDescend for " + childName + " when already have preSkip " + _postSkipDescend);

			if( _preSkipDescend == null)
				throw new UnexpectedVisitDataException( "postSkipDescend for " + childName + " when we haven't seen a preSkip");

			if( !childName.equals( _preSkipDescend))
				throw new UnexpectedVisitDataException( "postSkipDescend for " + childName + " when we have a different preSkip " + _preSkipDescend);

			_postSkipDescend = childName;
		}



		/**
		 * Whether this StateComponent is satisfied after running the visitor.
		 * @param myPath the name for this path element: only used to enhance error messages.
		 * @return
		 */
		boolean isGood( StatePath path, StatePath skip) {

			if( _type == StateComponentType.BRANCH) {
				return isGoodBranch( path, skip);
			} else {
				if( !_haveSeen) {
					System.out.println( "["+path.toString() + "] missing metric");
					return false;
				}
			}

			return true;
		}

		/**
		 * Given the current element is a branch, are we satisfied with the visit?
		 *
		 *   There are four cases to consider:
		 *     1. There is no skip, or skip and path are equal, or path is a child of skip,
		 *     2. path is parent of skip path,
		 *     3. path is an ancestor of skip path but not parent,
		 *     4. path is outside skip path.
		 *
		 *   What we expect to see:
		 *     1. visiting all children
		 *     2. visiting the child that's the next path element,
		 *     3. preSkip and postSkip the child that's the next path element,
		 *     4. no sub-elements are visited.
		 *
		 * @param path
		 * @param skip
		 * @param emitError
		 * @return
		 */
		boolean isGoodBranch( StatePath path, StatePath skip) {
			String pathName = path != null ? path.toString() : "(root)";

			/**
			 *  Case 1 (path is equal to skip path or is a child), or we have no skip.
			 *
			 *  We expect: all children are visited.
			 */
			if( skip == null || (path != null && skip.equalsOrHasChild(path))) {

				if( _preDescend.size() != _branchChildren) {
					System.out.println( "["+pathName + "] missing children in preDescend: expected " + _children.size() + ", got " + _preDescend.size());
					return false;
				}

				if( _postDescend.size() != _branchChildren) {
					System.out.println( "["+pathName + "] missing children in postDescend: expected " + _children.size() + ", got " + _postDescend.size());
					return false;
				}

				if( _preSkipDescend != null) {
					System.out.println( "["+pathName + "] preSkip unexpectedly not null: " + _preSkipDescend);
					return false;
				}

				if( _postSkipDescend != null) {
					System.out.println( "["+pathName + "] postSkip unexpectedly not null: " + _preSkipDescend);
					return false;
				}

				if( _visitedChildren != _children.size()) {
					System.out.println( "["+pathName + "] missing visited children, expected "+ Integer.toString( _children.size())+", got "+Integer.toString( _visitedChildren));
					return false;
				}

				return true;
			}



			StatePath skipParent = skip.parentPath();


			/**
			 *  Case 2 (path is parent of skip).
			 *
			 *  We expect: visit the final element of the skip-path.
			 */
			if( skipParent == null ? path == null : skipParent.equals( path)) {

				String finalElement = skip.getLastElement();

				if( _children.containsKey( finalElement)) {

					ComponentInfo childInfo = _children.get( finalElement);

					if( childInfo._type == ComponentInfo.StateComponentType.BRANCH) {

						if( _preDescend.size() != 1) {
							System.out.println( "["+pathName + "] preDescend count: expected 1, got " + _preDescend.size());
							return false;
						}

						if( !_preDescend.contains( finalElement)) {
							System.out.println( "["+pathName + "] preDescend contained incorrect entry: expected " + finalElement + ", got " + _preDescend.iterator().next());
							return false;
						}

						if( _postDescend.size() != 1) {
							System.out.println( "["+pathName + "] postDescend count: expected 1, got " + _postDescend.size());
							return false;
						}

						if( !_postDescend.contains( finalElement)) {
							System.out.println( "["+pathName + "] postDescend contained incorrect entry: expected " + finalElement + ", got " + _postDescend.iterator().next());
							return false;
						}
					} else {
						if( _preDescend.size() != 0) {
							System.out.println( "["+pathName + "] preDescend count: expected 0, got " + _preDescend.size());
							return false;
						}

						if( _postDescend.size() != 0) {
							System.out.println( "["+pathName + "] postDescend count: expected 0, got " + _postDescend.size());
							return false;
						}
					}

					if( _visitedChildren != 1) {
						System.out.println( "["+pathName + "] number of visited children for skip-parent wrong: expected 1, got " + Integer.toString( _visitedChildren));
						return false;
					}

				} else {

					if( _preDescend.size() != 0) {
						System.out.println( "["+pathName + "] preDescend count: expected 0, got " + _preDescend.size());
						return false;
					}

					if( _postDescend.size() != 0) {
						System.out.println( "["+pathName + "] postDescend count: expected 0, got " + _postDescend.size());
						return false;
					}

					if( _visitedChildren != 0) {
						System.out.println( "["+pathName + "] number of visited children wrong: expected 0, got " + Integer.toString( _visitedChildren));
						return false;
					}
				}

				if( _preSkipDescend != null) {
					System.out.println( "["+pathName + "] preSkip unexpectedly not null: " + _preSkipDescend);
					return false;
				}

				if( _postSkipDescend != null) {
					System.out.println( "["+pathName + "] postSkip unexpectedly not null: " + _preSkipDescend);
					return false;
				}

				return true;
			}

			/**
			 *  Case 3 (path is some ancestor of skip).
			 *
			 *  We expect: path is an ancestor of skip path but not parent
			 */
			if( path == null || path.equalsOrHasChild( skipParent) ) {

				// TODO this is very ugly: StatePath should be extended to support this.
				String nextElement = skip._elements.get( path == null ? 0 : path._elements.size());
				String nextNextElement = skip._elements.get( path == null ? 1 : path._elements.size()+1);

				ComponentInfo childInfo = _children.get( nextElement);
				boolean childHasChild = false;

				if( childInfo != null && childInfo._children.containsKey( nextNextElement))
					childHasChild = true;

				if( _preDescend.size() != 0) {
					System.out.println( "["+pathName + "] preDescend count: expected 0, got " + _preDescend.size());
					return false;
				}

				if( _postDescend.size() != 0) {
					System.out.println( "["+pathName + "] postDescend count: expected 0, got " + _postDescend.size());
					return false;
				}

				if( childHasChild) {

					if( _visitedChildren != 1) {
						System.out.println( "["+pathName + "] number of visited children for skip-ancestor wrong: expected 1, got " + Integer.toString( _visitedChildren));
						return false;
					}

					if( _preSkipDescend == null) {
						System.out.println( "["+pathName + "] no preSkip");
						return false;
					}

					if( !_preSkipDescend.equals( nextElement)) {
						System.out.println( "["+pathName + "] preSkip with unexpected value: expected " + nextElement + ", got " + _preSkipDescend);
						return false;
					}

					if( _postSkipDescend == null) {
						System.out.println( "["+pathName + "] no postSkip");
						return false;
					}

					if( !_postSkipDescend.equals( nextElement)) {
						System.out.println( "["+pathName + "] postSkip with unexpected value: expected " + nextElement + ", got " + _postSkipDescend);
						return false;
					}

				} else {

					if( _visitedChildren != 0) {
						System.out.println( "["+pathName + "] number of visited children for skip-ancestor wrong: expected 0, got " + Integer.toString( _visitedChildren));
						return false;
					}

					if( _preSkipDescend != null) {
						System.out.println( "["+pathName + "] preSkip found in skip-ancestor");
						return false;
					}

					if( _postSkipDescend != null) {
						System.out.println( "["+pathName + "] postSkip found in skip-ancestor");
						return false;
					}
				}

				return true;
			}


			/**
			 *  Case 4 (path is outside of skip)
			 *
			 *  Since all other Cases above return, the code will only get here if
			 *  path lies outside skip.
			 */
			if( _preDescend.size() != 0) {
				System.out.println( "["+pathName + "] preDescend count: expected 0, got " + _preDescend.size());
				return false;
			}

			if( _postDescend.size() != 0) {
				System.out.println( "["+pathName + "] postDescend count: expected 0, got " + _postDescend.size());
				return false;
			}

			if( _visitedChildren != 0) {
				System.out.println( "["+pathName + "] number of visited children wrong: expected 0, got " + Integer.toString( _visitedChildren));
				return false;
			}

			if( _preSkipDescend != null) {
				System.out.println( "["+pathName + "] unexpectedly got a preSkip: " + _preSkipDescend);
				return false;
			}

			if( _postSkipDescend != null) {
				System.out.println( "["+pathName + "] unexpectedly got a postSkip: " + _postSkipDescend);
				return false;
			}

			return true;
		}


		/**
		 * Reset this StateComponent to a preVisit state, ready for another visit.
		 */
		void reset() {
			_haveSeen = false;
			_visitedChildren = 0;
			_preDescend.clear();
			_postDescend.clear();
			_preSkipDescend = null;
			_postSkipDescend = null;
		}
	}

	/** The root of our tree */
	private final ComponentInfo _info = new ComponentInfo();

	private boolean _encounteredException;
	private boolean _seenRootPreSkip, _seenRootPostSkip;
	private boolean _seenRootPre, _seenRootPost;
	private StatePath _skip;

	/**
	 * Create an new VerifyingVisitor that expects to find nothing.
	 */
	public VerifyingVisitor() {}

	/**
	 * Create a new VerifyingVisitor that expects a StateComposite (branch).
	 * This is equivalent to
	 * <code>
	 *   VerifyingVisitor visitor = new VerifyingVisitor();
	 *   visitor.addExpectedBranch( branchPath);
	 * </code>
	 * @param branchPath the StatePath of the expected branch.
	 */
	protected VerifyingVisitor(StatePath branchPath) {
		addExpectedBranch( branchPath);
	}

	/**
	 * Create a new VerifyingVisitor that expects a StateValue (a metric).
	 * This is equivalent to:
	 * <code>
	 *   VerifyingVisitor visitor = new VerifyingVisitor();
	 *   visitor.addExpectedMetric( metricPath, metricValue);
	 * </code>
	 * @param metricPath
	 * @param metricValue
	 */
	protected VerifyingVisitor( StatePath metricPath, StateValue metricValue) {
		addExpectedMetric( metricPath, metricValue);
	}

	/**
	 * Add information that we expect to see a certain branch
	 * @param branchPathStr
	 */
	public void addExpectedBranch( StatePath path) {
		getOrCreateBranch( path);
	}

	public void addExpectedMetric( StatePath path, StateValue metricValue) {
		ComponentInfo.StateComponentType type = ComponentInfo.StateComponentType.identify( metricValue);

		ComponentInfo branchInfo = getOrCreateBranch( path.parentPath());
		branchInfo.addMetric( path.getLastElement(), type, metricValue.toString());
	}

	/**
	 *  Reset all values so the visitor may be used again.
	 */
	public void reset() {
		_encounteredException = false;
		_seenRootPre = false;
		_seenRootPost = false;
		resetThisAndChildren( _info);
	}

	/**
	 * Reset the visitor, visit the structure and assert the visitor is satisfied with the result.
	 * @param msg
	 * @param root
	 * @param skip
	 */
	protected void assertSatisfied( String msg, StateComposite root) {
		assertSatisfiedSkip( msg, root, null);
	}

	/**
	 * Reset the visitor, visit the structure with supplied skip and assert the visitor is
	 * satisfied with the result.
	 * @param msg
	 * @param visitor
	 * @param root
	 * @param skip
	 */
	protected void assertSatisfiedSkip( String msg, StateComposite root, StatePath skip) {
		setSkip( skip);
		reset();
		root.acceptVisitor( null, skip, this);
		assertTrue( msg, satisfied());
	}

	/**
	 * Reset the visitor, visit the structure with supplied transition and assert the visitor is
	 * satisfied with the result.
	 * @param msg
	 * @param visitor
	 * @param root
	 * @param skip
	 */
	protected void assertSatisfiedTransition( String msg, StateComposite root, StateTransition transition) {
		assertSatisfiedTransitionSkip( msg, root, null, transition);
	}

	/**
	 * Reset the visitor, visit the structure with supplied transition and assert the visitor is
	 * satisfied with the result.
	 * @param msg
	 * @param visitor
	 * @param root
	 * @param skip
	 */
	protected void assertSatisfiedTransitionSkip( String msg, StateComposite root, StatePath skip, StateTransition transition) {
		setSkip( skip);
		reset();
		root.acceptVisitor( transition, null, skip, this);
		assertTrue( msg, satisfied());
	}




	/**
	 * Did we visit without encountering any exception and did we satisfy all expected elements?
	 * @return
	 */
	public boolean satisfied() {
		boolean haveSkip = _skip != null;

		if( !haveSkip && !_seenRootPre) {
			System.out.println( "Missing root pre descend");
			return false;
		}

		if( haveSkip && _seenRootPre) {
			System.out.println( "Unexpected root pre descend");
			return false;
		}

		if( !haveSkip && !_seenRootPost) {
			System.out.println( "Missing root post descend");
			return false;
		}

		if( haveSkip && _seenRootPost) {
			System.out.println( "Unexpected root post descend");
			return false;
		}

		if( haveSkip && !_seenRootPreSkip) {
			System.out.println( "Missing root pre skip descend");
			return false;
		}

		if( !haveSkip && _seenRootPreSkip) {
			System.out.println( "Unexpected root pre skip descend");
			return false;
		}

		if( haveSkip && !_seenRootPostSkip) {
			System.out.println( "Missing root post skip descend");
			return false;
		}

		if( !haveSkip && _seenRootPostSkip) {
			System.out.println( "Unexpected root post skip descend");
			return false;
		}



		if( _encounteredException)
			return false;

		return isGood( _info, _skip, null);
	}




	/**
	 *
	 * METHODS TO SATISFY VISITOR PATTERN
	 *
	 * 		Metrics:
	 */

	public void visitString( StatePath path, StringStateValue value) {
		markMetric( path, ComponentInfo.StateComponentType.STRING, value.toString());
	}

	public void visitInteger( StatePath path, IntegerStateValue value) {
		markMetric( path, ComponentInfo.StateComponentType.INTEGER, value.toString());
	}

	public void visitBoolean( StatePath path, BooleanStateValue value) {
		markMetric( path, ComponentInfo.StateComponentType.BOOLEAN, value.toString());
	}

	public void visitFloatingPoint( StatePath path, FloatingPointStateValue value) {
		markMetric( path, ComponentInfo.StateComponentType.FLOATINGPOINT, value.toString());
	}


	/**
	 * 		... and branches:
	 */

	public void visitCompositePreDescend( StatePath path, Map<String,String> metadata) {
		ComponentInfo thisBranch;

		if( path == null) {
			if( _skip == null)
				_seenRootPre = true;
			else
				System.out.println("Seen PreDesc for root with registered skip");
			return;
		}

		try {
			thisBranch = getBranch( path.parentPath());
			thisBranch.markPreDescend( path.getLastElement());
		} catch (UnexpectedVisitDataException e) {
			System.out.println( e.getMessage());
			_encounteredException = true;
		}
	}

	public void visitCompositePostDescend( StatePath path, Map<String,String> metadata) {
		ComponentInfo thisBranch;

		if( path == null) {
			if( _skip == null)
				_seenRootPost = true;
			else
				System.out.println("Seen PostDesc for root with registered skip");
			return;
		}

		try {
			thisBranch = getBranch(path.parentPath());
			thisBranch.markPostDescend(path.getLastElement());
		} catch (UnexpectedVisitDataException e) {
			System.out.println( e.getMessage());
			_encounteredException = true;
		}
	}


	/** StateComposites call these methods when skipping over higher-level state */
	public void visitCompositePreSkipDescend( StatePath path, Map<String,String> metadata) {
		ComponentInfo thisBranch;

		if( path == null) {
			_seenRootPreSkip = true;
			return;
		}

		try {
			thisBranch = getBranch(path.parentPath());
			thisBranch.markPreSkipDescend(path.getLastElement());
		} catch (UnexpectedVisitDataException e) {
			System.out.println( e.getMessage());
			_encounteredException = true;
		}
	}

	public void visitCompositePostSkipDescend( StatePath path, Map<String,String> metadata) {
		ComponentInfo thisBranch;

		if( path == null) {
			_seenRootPostSkip = true;
			return;
		}

		try {
			thisBranch = getBranch(path.parentPath());
			thisBranch.markPostSkipDescend(path.getLastElement());
		} catch (UnexpectedVisitDataException e) {
			System.out.println( e.getMessage());
			_encounteredException = true;
		}
	}


	/**
	 *  P R I V A T E   M E T H O D S
	 */

	/**
	 * Obtain the ComponentInfo corresponding to the given path; if path is null
	 * then the root branch (_info) is returned.
	 * @param path the path to the branch to obtain
	 * @return the ComponentInfo for this branch
	 * @throws UnexpectedVisitDataException if any part of the path doesn't exist
	 */
	private ComponentInfo getBranch( StatePath path) throws UnexpectedVisitDataException {
		ComponentInfo thisComponent = _info;

		for(; path != null; path = path.childPath())
			thisComponent = thisComponent.getChild( path.getFirstElement());

		return thisComponent;
	}

	/**
	 * Given a StatePath, return the ComponentInfo for this branch.
	 * @param branchPath the dot-separated path.
	 * @return the corresponding ComponentInfo for this branch.
	 */
	private ComponentInfo getOrCreateBranch( StatePath path) {
		ComponentInfo thisComponent = _info;

		for(; path != null; path = path.childPath())
			thisComponent = thisComponent.getOrCreateBranch( path.getFirstElement());

		return thisComponent;
	}



	/**
	 * Is the supplied ComponentInfo and all its children "good"?
	 * @param info the top branch of the hierarchy
	 * @param path the path to info (used to decorate any error messages)
	 * @return true if this ComponentInfo and all children are "good", false otherwise.
	 */
	private boolean isGood( ComponentInfo info, StatePath skip, StatePath path) {

		if( !info.isGood( path, skip))
			return false;

		for( Entry<String,ComponentInfo> e : info._children.entrySet()) {
			String childName = e.getKey();
			ComponentInfo childComponentInfo = e.getValue();

			StatePath childPath = path != null ? path.newChild( childName) : new StatePath( childName);

			if( !isGood( childComponentInfo, skip, childPath))
				return false;
		}

		return true;
	}


	/**
	 * Reset the given ComponentInfo to a pre-visited state and all
	 * of the children.
	 * @param info the ComponentInfo root of the tree to reset
	 */
	private void resetThisAndChildren( ComponentInfo info) {
		info.reset();

		for( ComponentInfo childInfo : info._children.values())
			resetThisAndChildren( childInfo);
	}

	/**
	 * Mark a metric as found, verifying it matches the expected value.
	 * @param path the path to the metric
	 * @param type the metric type
	 * @param value the encountered metric value
	 * @throws UnexpectedVisitDataException if there is no expected metric, it differs in some what or has already been marked.
	 */
	private void markMetric( StatePath path, ComponentInfo.StateComponentType type, String value) {

		try {
			ComponentInfo metricBranchInfo = getBranch( path.parentPath());

			metricBranchInfo.markMetric( path.getLastElement(), type, value);
		} catch( UnexpectedVisitDataException e) {
			System.out.println( "["+path.toString() + "] " + e.getMessage());
			_encounteredException = true;
		}
	}

	/**
	 * Register the path we will skip when visiting.
	 * @param path the StatePath we anticipate skipping in next visit or null if no
	 * skipping is to be done.
	 */
	public void setSkip( StatePath path) {
		_skip = path;
	}


}
