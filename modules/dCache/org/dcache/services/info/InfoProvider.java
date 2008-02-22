package org.dcache.services.info;

import java.util.*;
import dmg.cells.nucleus.*;
import dmg.util.Args;
//import dmg.util.Args;
import org.dcache.services.info.base.*;
import org.dcache.services.info.conduits.*;
import org.dcache.services.info.gathers.*;
import org.dcache.services.info.serialisation.*;

public class InfoProvider extends CellAdapter {
	
	private static InfoProvider _instance=null;
	private static final String ADMIN_INTERFACE_OK = "Done.";
	public static final String ADMIN_INTERFACE_NONE = "(none)";
	public static final String ADMIN_INTERFACE_LIST_PREFIX = "  ";
	
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
	private StatePath _startSerialisingFrom = null;
	
	
	public InfoProvider(String name, String argstr) {
		
		super(name, argstr, false);
		
        /**
         * TODO: should be done with cell commands
         */
        setPrintoutLevel( CellNucleus.PRINT_CELL |
                          CellNucleus.PRINT_ERROR_CELL ) ;


		say( "InfoProvider starting...");
		
		if( _instance == null)
			_instance = this;
		else {
			say( "Duplicate InfoProvider detected.");
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
	    startConduits();
		
		start();  // Go, go gadget InfoProvider
		export();
	}
	

	/**
	 * Called from the Cell's finalize() fn.
	 */
	public void cleanUp() {
		stopConduits();
		_scheduler.shutdown();
	}

	
	/**
	 *    C O N D U I T S
	 */
	
	
	/**
	 *  Start all known conduits
	 */
	void startConduits() {
		for( Iterator<Conduit> itr = _conduits.values().iterator(); itr.hasNext();)
			itr.next().enable();
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
		for( Iterator<Conduit> itr = _conduits.values().iterator(); itr.hasNext();)
			itr.next().disable();		
	}
	
	private String enableConduit( String name) {
		Conduit con = _conduits.get(name);
		
		if( con == null)
			return "Conduit " + name + " was not found.";
		
		if( con.isEnabled())
			return "Conduit " + name + " is already enabled.";
		
		con.enable();
		
		return null;
	}
	
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
        ict.start();
        ict.setName("Data-Gathering thread");
	}

	
	/**
	 * Instantiate our MessageHandlerChain and populate it with a default
	 * set of MessageHandler subclass instances.
	 */
	private void buildMessageHandlerChain() {
		_msgHandlerChain = new MessageHandlerChain();
		
		//TODO: add default Handlers.
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
		if( !_msgHandlerChain.handleMessage( msg))
			esay( "Unable to handle incoming message from: " + msg.getSourceAddress());
	}

	/**
	 * Override the CellAdapter default sendMessage() so we simply display
	 * an error message if we get an exception.
	 */
	public void sendMessage( CellMessage msg) {
		try { 
			super.sendMessage(msg);
		} catch(Exception e ) {
			esay( "Problem sending msg: "+e );
		}
	}
	
	
	/**
	 *   S E R I A L I S E R S
	 */
	private void addSerialiser( StateSerialiser serialiser) {
		_availableSerialisers.put( serialiser.getName(), serialiser);
	}
	
	/**
	 *   H A N D L E R    A D M I N    C O M M A N D S
	 */
	
	public String fh_handler_ls = "List all known Message handlers.  These are responsible for updating dCache state.";
    public String hh_handler_ls = "";
	public String ac_handler_ls_$_0( Args args ) {		
		StringBuffer sb = new StringBuffer();
		
		sb.append( "Incoming Message Handlers:\n");
		sb.append( _msgHandlerChain.listMessageHandlers());
		
		return sb.toString();
	}

	
	/**
	 *   C O N D U I T   A D M I N    C O M M A N D S
	 */
	
	
	public String fh_conduits_ls = "List all known conduits.  Conduits provide read-only access to dCache current state.";
	public String ac_conduits_ls_$_0( Args args) {
		StringBuffer sb = new StringBuffer();
		
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
		StringBuffer sb = new StringBuffer();
		sb.append( "Data-Gathering Activity:\n");
		sb.append( _scheduler.listActivity());
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
		StringBuffer sb = new StringBuffer();		
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
		StringBuffer sb = new StringBuffer();

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
		StringBuffer sb = new StringBuffer();
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
		StringBuffer sb = new StringBuffer();
		
		sb.append( "dCache");
		
		if( _startSerialisingFrom != null) {
			sb.append( ".");
			sb.append( _startSerialisingFrom.toString());
		}
		
		return sb.toString();
	}
	
	public String fh_state_cd = "Change directory for state ls; path elements must be slash-seperated";
	public String hh_state_cd = "<path>";
	public String ac_state_cd_$_1( Args args) {
		
		StatePath newPath;
		
		try {
			newPath = processPath( _startSerialisingFrom, args.argv(0));
		} catch( BadStatePathException e) {
			return e.toString();
		}

		_startSerialisingFrom = newPath;

		StringBuffer sb = new StringBuffer();
		sb.append( "Path now: ");
		sb.append(ac_state_pwd_$_0(null));
		return sb.toString();
	}
	
	
	
	/**
	 * Create a new StatePath based on a current path and a description of the new path.
	 * The description of the path may be relative or absolute.  Absolute paths start
	 * with '/', all other paths are relative.  The elements '.' and '..' have the
	 * same meaning as in Unix FS.
	 * 
	 * @param cwd current path
	 * @param path description of new path
	 * @return a new path, or null if the path is root
	 * @throws BadPathException if the relative path is impossible.
	 */
	private StatePath processPath( StatePath cwd, String path) throws BadStatePathException {
		String[] pathElements;
		StatePath currentPath = cwd;
		
		/* Treat a quoted arg as a single entry--don't split */ 
		if( path.startsWith("\"") && path.endsWith("\"")) {
			pathElements = new String[1];
			pathElements[1] = path.substring(1, path.length()-2);
		} else {			
			if( path.startsWith("/"))
				currentPath = null; // cd is with absolute path, so reset our path.
			
			pathElements = path.split( "/");
		}
		
		for( int i = 0; i < pathElements.length; i++) {
		
			if( pathElements[i].equals("..")) {
				// Ascend once in the hierachy.				
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
		StringBuffer sb = new StringBuffer();
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
}
