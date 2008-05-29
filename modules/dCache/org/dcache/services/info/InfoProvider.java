package org.dcache.services.info;

import java.io.NotSerializableException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.BadStatePathException;
import org.dcache.services.info.base.State;
import org.dcache.services.info.base.StateMaintainer;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.conduits.Conduit;
import org.dcache.services.info.conduits.XmlConduit;
import org.dcache.services.info.gathers.DataGatheringScheduler;
import org.dcache.services.info.gathers.MessageHandlerChain;
import org.dcache.services.info.secondaryInfoProviders.PoolgroupSpaceWatcher;
import org.dcache.services.info.serialisation.PrettyPrintTextSerialiser;
import org.dcache.services.info.serialisation.SimpleTextSerialiser;
import org.dcache.services.info.serialisation.StateSerialiser;
import org.dcache.services.info.serialisation.XmlSerialiser;

import org.dcache.vehicles.InfoGetSerialisedDataMessage;

public class InfoProvider extends CellAdapter {
	
	private static Logger _log = Logger.getLogger(InfoProvider.class);
	
	/** Our default timeout for sending messages, in milliseconds */
	private static final long STANDARD_TIMEOUT = 1000;

	
	private static InfoProvider _instance;
	private static final String ADMIN_INTERFACE_OK = "Done.";
	private static final String ADMIN_INTERFACE_NONE = "(none)";
	private static final String ADMIN_INTERFACE_LIST_PREFIX = "  ";
	private static final String TOPLEVEL_DIRECTORY_LABEL = "(top)";

	
	/**
	 * Poor man's singleton
	 * @return
	 */
	public static InfoProvider getInstance() {
		return _instance;
	}
	
	private Map<String,Conduit> _conduits;
	private DataGatheringScheduler _scheduler;
	private MessageHandlerChain _msgHandlerChain;
	private StateSerialiser _currentSerialiser = new SimpleTextSerialiser();
	private Map<String,StateSerialiser> _availableSerialisers;
	private StatePath _startSerialisingFrom;
	
	
	public InfoProvider(String name, String argstr) {
		
		super(name, argstr, false);
		
        setPrintoutLevel( CellNucleus.PRINT_CELL |
                          CellNucleus.PRINT_ERROR_CELL ) ;

		_log.info( "InfoProvider starting...");
		
		if( _instance == null)
			_instance = this;
		else {
			_log.warn( "Duplicate InfoProvider detected.");
		}

		/**
		 * Build our list of possible serialisers.
		 */
		_availableSerialisers = new HashMap<String,StateSerialiser>();	
		addSerialiser( new XmlSerialiser());
		addSerialiser( new SimpleTextSerialiser());
		addSerialiser( new PrettyPrintTextSerialiser());

		useInterpreter( true );
	    buildMessageHandlerChain();
	    startDgaScheduler();
		addDefaultConduits();
		addDefaultWatchers();
	    startConduits();
		
		start();  // Go, go gadget InfoProvider
		export();
	}
	

	/**
	 * Called from the Cell's finalize() method.
	 */
	public void cleanUp() {
		stopConduits();
		_scheduler.shutdown();
		StateMaintainer.getInstance().shutdown();
	}

	
	/**
	 *    C O N D U I T S
	 */
	
	
	/**
	 *  Start all known conduits
	 */
	void startConduits() {
		for( Conduit conduit : _conduits.values())
			conduit.enable();
	}
	
	
	/**
	 *   Initialise conduits list with default options.
	 */
	void addDefaultConduits() {
		_conduits = new HashMap<String,Conduit>();
		
		Conduit con = new XmlConduit();
		_conduits.put( con.toString(), con);		
	}
	
	
	/**
	 * Stop any started conduits.
	 */
	void stopConduits() {
		for( Conduit conduit : _conduits.values())
			conduit.disable();		
	}
	
	
	/**
	 * Switch on "enable" a named conduit.
	 * @param name the name of the conduit to enable.
	 * @return null if there was no problem, or a description of the problem otherwise.
	 */
	private String enableConduit( String name) {
		Conduit con = _conduits.get(name);
		
		if( con == null)
			return "Conduit " + name + " was not found.";
		
		if( con.isEnabled())
			return "Conduit " + name + " is already enabled.";
		
		con.enable();
		
		return null;
	}
	
	/**
	 * Attempt to disable a named conduit.
	 * @param name the name of the conduit to disable
	 * @return null if there was no problem, a description of the problem otherwise.
	 */
	private String disableConduit( String name) {
		Conduit con = _conduits.get( name);
		
		if( con == null)
			return "Conduit " + name + " was not found.";
		
		if( !con.isEnabled())
			return "Conduit " + name + " is not currently enabled.";
		
		con.disable();
		
		return null;
	}

	
	
	/**
	 *    D A T A---G A T H E R I N G   A C T I V I T Y
	 */
	


	/**
	 *  Start the synchronous collection of dCache state: Data-Gathering Activity.
	 *  This requires that MessageHandlerChain has already been initialised.
	 */
	void startDgaScheduler()
	{
	    _scheduler = new DataGatheringScheduler();

	    _scheduler.addDefaultActivity();

        Thread ict = new Thread( _scheduler);
        ict.setName("DGA-Scheduler");
        ict.start();
	}

	
	/**
	 * Instantiate our MessageHandlerChain and populate it with a default
	 * set of MessageHandler subclass instances.
	 */
	private void buildMessageHandlerChain() {
		_msgHandlerChain = new MessageHandlerChain();
			
		// TODO: add default Message Handlers.
		//_msgHandlerChain.addDefaultHandlers();
	}
	
	
	/**
	 * Provide the InfoProvider's MessageHandlerChain.
	 * @return the MessageHandlerChain instance.
	 */
	public MessageHandlerChain getMessageHandlerChain() {
		return _msgHandlerChain;
	}

		
	/**
	 * The method called when this cell receives a new message.  We devolve all
	 * responsibility for processing this CellMessage to our MessageHandlerChain
	 * instance.
	 */
	public synchronized void messageArrived( CellMessage msg ) {
		
		if( msg.getMessageObject() instanceof InfoGetSerialisedDataMessage) {
			addSerialisedDataToMsg( (InfoGetSerialisedDataMessage) msg.getMessageObject());
			
	    	msg.revertDirection();
    		try {
				super.sendMessage(msg);
			} catch (NotSerializableException e) {
				// This should never happen.
				_log.error( "unable to serialise reply message");
			} catch (NoRouteToCellException e) {
				// This can happen if the querying cell dies whilst we are preparing the serialised output.
				_log.warn("can't send reply message to "+ msg.getDestinationPath() + " : " + e.getMessage());
			}

			return;
		}
		
		if( !_msgHandlerChain.handleMessage( msg))
			_log.warn( "Unable to handle incoming message from: " + msg.getSourceAddress());
	}

	/**
	 * Override the CellAdapter default sendMessage() so we simply display
	 * an error message if we get an exception.
	 */
	public void sendMessage( CellMessage msg) {
		try { 
			super.sendMessage(msg);
		} catch( NotSerializableException e) {
			_log.warn( "Message could not be serialised (this should never happen) ", e);
			throw( new RuntimeException(e));
		} catch( NoRouteToCellException e) {
			_log.info( "Cannot route message to cell, refraining from delivering msg.", e);
		}
	}

	/**
	 * Override the CellAdapter default sendMessage() so we simply display
	 * an error message if we get an exception
	 * @param msg
	 * @param callback
	 */
	public void sendMessage( CellMessage msg, CellMessageAnswerable callback) {
		try { 
			super.sendMessage(msg, callback, STANDARD_TIMEOUT);
		} catch(Exception e ) {
			_log.warn( "Problem sending msg: ", e);
		}
	}

	
	/**
	 *   S E R I A L I S E R S
	 */
	private void addSerialiser( StateSerialiser serialiser) {
		_availableSerialisers.put( serialiser.getName(), serialiser);
	}
	
	/**
	 *   S T A T E   W A T C H E R S
	 */
	private void addDefaultWatchers() {
		State.getInstance().addStateWatcher(new PoolgroupSpaceWatcher());
	}
	
	
	/**
	 *   H A N D L E R    A D M I N    C O M M A N D S
	 */
	
	public String fh_handler_ls = "List all known Message handlers.  These are responsible for updating dCache state.";
    public String hh_handler_ls = "";
	public String ac_handler_ls_$_0( Args args ) {		
		StringBuilder sb = new StringBuilder();
		
		sb.append( "Incoming Message Handlers:\n");
		String msgHandlers[] = _msgHandlerChain.listMessageHandlers();
		if( msgHandlers.length > 0) {
			for( String msgHandler : msgHandlers) {
				sb.append( ADMIN_INTERFACE_LIST_PREFIX);
				sb.append( msgHandler);
				sb.append( "\n");
			}
		} else {
			sb.append( ADMIN_INTERFACE_LIST_PREFIX);
			sb.append( ADMIN_INTERFACE_NONE);
			sb.append( "\n");
		}

		return sb.toString();
	}

	
	/**
	 *   C O N D U I T   A D M I N    C O M M A N D S
	 */
	
	
	public String fh_conduits_ls = "List all known conduits.  Conduits provide read-only access to dCache current state.";
	public String ac_conduits_ls_$_0( Args args) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Conduits:\n");
		
		if( _conduits.size() > 0)
			for( Conduit con : _conduits.values()) {
				sb.append( ADMIN_INTERFACE_LIST_PREFIX);
				sb.append( con.toString());
				sb.append( "  ");
				sb.append( con.getInfo());
				sb.append( "\n");
			}
		else {
			sb.append( ADMIN_INTERFACE_LIST_PREFIX);
			sb.append( ADMIN_INTERFACE_NONE);
			sb.append( "\n");
		}
		
		return sb.toString();
	}
	
	public String fh_conduits_enable = "Enabled the named conduit.";
	public String hh_conduits_enable = "<conduit name>";
	public String ac_conduits_enable_$_1( Args args) {
		String errMsg = enableConduit( args.argv(0));
		return errMsg == null ? ADMIN_INTERFACE_OK : errMsg;
	}

	public String fh_conduits_disable = "Disable the named conduit.";
	public String hh_conduits_disable = "<conduit name>";
	public String ac_conduits_disable_$_1( Args args) {
		String errMsg = disableConduit( args.argv(0));
		return errMsg == null ? ADMIN_INTERFACE_OK : errMsg;
	}
	

	/**
	 *   D G A   A D M I N   C O M M A N D S
	 */
	
	public String fh_dga_ls = "list all known data-gathering activity, whether enabled or not.";
	public String ac_dga_ls_$_0( Args args) {
		StringBuilder sb = new StringBuilder();
		sb.append( "Data-Gathering Activity:\n");
		List<String> dgaList = _scheduler.listActivity();
		
		if( dgaList.size() > 0)
			for( String activity : dgaList) {
				sb.append( ADMIN_INTERFACE_LIST_PREFIX);
				sb.append(activity);
				sb.append( "\n");
			}
		else {
			sb.append( ADMIN_INTERFACE_LIST_PREFIX);
			sb.append( ADMIN_INTERFACE_NONE);
			sb.append( "\n");
		}

		return sb.toString();
	}
	
	public String hh_dga_disable = "<name>";
	public String fh_dga_disable = "disable a data-gathering activity.";
	public String ac_dga_disable_$_1( Args args) {
		String errMsg = _scheduler.disableActivity(args.argv(0));
		return errMsg == null ? ADMIN_INTERFACE_OK : errMsg;
	}
	
	public String hh_dga_enable = "<name>";
	public String fh_dga_enable = "enable a data-gathering activity.  The next trigger time is randomly chosen.";
	public String ac_dga_enable_$_1( Args args) {
		String errMsg = _scheduler.enableActivity( args.argv(0));
		return errMsg == null ? ADMIN_INTERFACE_OK : errMsg;
	}
	
	public String hh_dga_trigger = "<name>";
	public String fh_dga_trigger = "trigger data-gathering activity <name> now.";
	public String ac_dga_trigger_$_1( Args args) {
		String errMsg = _scheduler.triggerActivity( args.argv(0));
		return errMsg == null ? ADMIN_INTERFACE_OK : errMsg;
	}
	
	
	/**
	 *   S T A T E   A D M I N   C O M M A N D S
	 */
	
	public String fh_state_ls = "List current status of dCache";
	public String hh_state_ls = "[<path>]";
	public String ac_state_ls_$_0_1( Args args) {
		StringBuilder sb = new StringBuilder();		
		StatePath start = _startSerialisingFrom;
		
		if( args.argc() == 1) {
			try {
				start = processPath( _startSerialisingFrom, args.argv(0));
			} catch( BadStatePathException e) {
				return e.toString();
			} 
		}
		
		sb.append("\n");
		
		if( start != null)
			sb.append( _currentSerialiser.serialise( start));
		else
			sb.append( _currentSerialiser.serialise());
		
		if( sb.length() > 1)
			sb.append("\n");
		
		return sb.toString();
	}
	
	public String fh_state_output = "view or change output format for the \"state ls\" command.";
	public String hh_state_output = "[<format>]";
	public String ac_state_output_$_0_1( Args args) {
		StringBuilder sb = new StringBuilder();

		if( args.argc() == 0) {
			
			sb.append( "Current output: ");
			sb.append( _currentSerialiser.getName());
			sb.append( "\n");
			sb.append( list_valid_output());
			
		} else {
			
			StateSerialiser newSerialiser = _availableSerialisers.get(args.argv(0));
			
			if( newSerialiser != null) {
				_currentSerialiser = newSerialiser;
				sb.append( "Will use ");
				sb.append( _currentSerialiser.getName());
				sb.append( " formatting for future output.");
			} else {
				sb.append( "Unknown output format \"");
				sb.append( args.argv(0));
				sb.append( "\".\n");
				sb.append( list_valid_output());
			}
		}
		
		return sb.toString();
	}

	
	private String list_valid_output() {
		StringBuilder sb = new StringBuilder();
		sb.append( "Valid output format");
		sb.append( (_availableSerialisers.size() > 1) ? "s are" : " is");
		sb.append( ": ");
		for( Iterator<String> itr = _availableSerialisers.keySet().iterator(); itr.hasNext();) {
			sb.append( itr.next());
			if( itr.hasNext())
				sb.append( ", ");
		}
		sb.append("\n");
		
		return sb.toString();
	}
	
	public String fh_state_pwd = "List the current directory for state ls";
	public String ac_state_pwd_$_0( Args args) {
		StringBuilder sb = new StringBuilder();
		
		if( _startSerialisingFrom != null)
			sb.append( _startSerialisingFrom.toString());
		else
			sb.append(TOPLEVEL_DIRECTORY_LABEL);
		
		return sb.toString();
	}
	
	public String fh_state_cd = "Change directory for state ls; path elements must be slash-separated";
	public String hh_state_cd = "<path>";
	public String ac_state_cd_$_1( Args args) {
		
		StatePath newPath;
		
		try {
			newPath = processPath( _startSerialisingFrom, args.argv(0));
		} catch( BadStatePathException e) {
			return e.toString();
		}

		_startSerialisingFrom = newPath;

		StringBuilder sb = new StringBuilder();
		sb.append( "Path now: ");
		sb.append(ac_state_pwd_$_0(null));
		return sb.toString();
	}
	
	
	
	/**
	 * Create a new StatePath based on a current path and a description of the new path.
	 * The description of the path may be relative or absolute.  The separator between path
	 * elements is a forward slash ("/").   Absolute paths start with '/', all other paths
	 * are relative. 
	 * <p>
	 * Two special paths are also understood: "." and "..".  The "." path is always the current
	 * element and ".." the parent element.  This allows for a filesystem-like paths to be
	 * expressed, where sibling elements can be navigated to, using a combination of ".." and
	 * the sibling path-element name.
	 * <p>
	 * Paths are normally displayed as a dot-separated list of elements.  This may lead to
	 * confusion as, to change directory to <tt>aaa.bbb.ccc</tt> one would specify (as an absolute
	 * path) <tt>/aaa/bbb/ccc</tt> or some path relative to the current location.
	 * <p>
	 * To allow users to "type what they see", a special case is introduced.  If the path has
	 * no forward-slash and is neither of the two special elements ("." and ".."), then
	 * the path is treated as relative, with dot-separated list of elements.
	 * <p>
	 * One final refinement is the presence of quote marks around the element.  If the first and last
	 * characters are double-quote marks, then the contents is treated as a single path element
	 * and no further special treatment is taken. 
	 * 
	 * @param cwd current path
	 * @param path description of new path
	 * @return a new path, or null if the path is root
	 * @throws BadPathException if the relative path is impossible.
	 */
	private StatePath processPath( StatePath cwd, String path) throws BadStatePathException {
		String[] pathElements;
		StatePath currentPath = cwd;
		boolean quoted = false;
		
		/* Treat a quoted argument as a single entry--don't split */ 
		if( path.startsWith("\"") && path.endsWith("\"")) {
			pathElements = new String[1];
			pathElements[1] = path.substring(1, path.length()-2);
			quoted = true;
		} else {			
			if( path.startsWith("/"))
				currentPath = null; // cd is with absolute path, so reset our path.
			
			pathElements = path.split( "/");
		}

		/**
		 * As a special case: no slashes, single element in list containing dots that 
		 * isn't "." or "..".  Treat this as a relative path, splitting on the dots.
		 */
		if( !quoted && pathElements.length == 1) {
			String element = pathElements [0];
			
			if( !element.contains( "/") && element.contains(".") && !element.equals(".") && !element.equals("..")) {
				if( currentPath != null)
					currentPath = currentPath.newChild( StatePath.parsePath( element));
				else
					currentPath = StatePath.parsePath( element);
				
				return currentPath;
			}
		}

		
		for( int i = 0; i < pathElements.length; i++) {
		
			if( pathElements[i].equals("..")) {
				// Ascend once in the hierarchy.				
				if( currentPath != null)
					currentPath = currentPath.parentPath();
				else
					throw( new BadStatePathException("You cannot cd upward from the top-most element."));
			} else if (pathElements[i].equals( ".")) {
				// Do nothing, just to emulate Unix FS semantics. 
			} else {
				if( pathElements[i].length() > 0) {
					if( currentPath == null)
						currentPath = new StatePath( pathElements[i]);
					else
						currentPath = currentPath.newChild( pathElements[i]);
				} else {
					if( i == 0) 
						currentPath = null; // ignore initial empty element from absolute paths 
					else
						throw( new BadStatePathException( "Path contains zero-length elements."));
				}
			}
		}

		return currentPath;
	}

	
	/**
	 *  W A T C H E R    A D M I N    C O M M A N D S
	 */
	public String fh_watcher_ls = "list all registered dCache state watchers";
	public String ac_watcher_ls_$_0( Args args) {
		StringBuilder sb = new StringBuilder();
		sb.append( "State Watchers:\n");
		String watcherNames[] = State.getInstance().listStateWatcher();
		
		if( watcherNames.length > 0)
			for( String name : watcherNames) {
				sb.append( ADMIN_INTERFACE_LIST_PREFIX);
				sb.append( name);
				sb.append( "\n");
			}
		else {
			sb.append( ADMIN_INTERFACE_LIST_PREFIX);
			sb.append( ADMIN_INTERFACE_NONE);
			sb.append( "\n");
		}
			
		return sb.toString();
	}

	/**
	 * Satisfy an incoming request for serialised dCache state.
	 * 
	 * @param msg the Message (Vehicle) requesting the data.
	 */
	private void addSerialisedDataToMsg( InfoGetSerialisedDataMessage msg) {

		if( _log.isInfoEnabled())
			_log.info("Received InfoGetSerialisedDataMessage.");

		StateSerialiser xmlSerialiser = _availableSerialisers.get( XmlSerialiser.NAME);
		String data;
		
		if( xmlSerialiser != null) {
			
			data = msg.isCompleteDump() ? xmlSerialiser.serialise() : xmlSerialiser.serialise( StatePath.buildFromList( msg.getPathElements()));
			
		} else {
			_log.error("Couldn't find the xmlSerialiser");
			
			// Really, we should propagate this back as an Exception.
			data = null;
		}
		
		msg.setData( data);		
	}

}
