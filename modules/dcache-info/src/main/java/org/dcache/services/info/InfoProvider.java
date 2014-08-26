package org.dcache.services.info;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.services.info.base.BadStatePathException;
import org.dcache.services.info.base.State;
import org.dcache.services.info.base.StateObservatory;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.conduits.Conduit;
import org.dcache.services.info.gathers.DataGatheringScheduler;
import org.dcache.services.info.gathers.MessageHandlerChain;
import org.dcache.services.info.serialisation.SimpleTextSerialiser;
import org.dcache.services.info.serialisation.StateSerialiser;
import org.dcache.services.info.serialisation.XmlSerialiser;
import org.dcache.util.Args;
import org.dcache.vehicles.InfoGetSerialisedDataMessage;

public class InfoProvider  implements CellCommandListener, CellInfoProvider,
               CellMessageReceiver
{
	private static Logger _log = LoggerFactory.getLogger(InfoProvider.class);

	private static final String ADMIN_INTERFACE_OK = "Done.";
	private static final String ADMIN_INTERFACE_NONE = "(none)";
	private static final String ADMIN_INTERFACE_LIST_PREFIX = "  ";
	private static final String TOPLEVEL_DIRECTORY_LABEL = "(top)";


    private String _defaultSerialiser = SimpleTextSerialiser.NAME;

	private Map<String,Conduit> _conduits;
	private DataGatheringScheduler _scheduler;
	private MessageHandlerChain _msgHandlerChain;
	private StateSerialiser _currentSerialiser;
	private Map<String,StateSerialiser> _availableSerialisers;
	private StatePath _startSerialisingFrom;
    private State _state;
    private StateObservatory _observatory;

    /**
     * Provide information for the info command.
     */
    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("    Overview of the info cell:\n");

        pw.print( _conduits.size());
        pw.print( " conduit"+ (_conduits.size()==1?"":"s") + " (");
        int count=0;
        for( Conduit c : _conduits.values()) {
            count += c.isEnabled() ? 1 : 0;
        }
        pw.print( count);
        pw.println( " enabled)");

        pw.print( _scheduler.listActivity().size());
        pw.println( " data-gathering activities.");

        pw.print( _availableSerialisers.size());
        pw.println( " available serialisers.");

        _state.getInfo( pw);
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }


    @Required
    public void setState(State state)
    {
        _state = state;
    }

    @Required
    public void setStateObservatory(StateObservatory observatory)
    {
        _observatory = observatory;
    }

    @Required
    public void setDataGatheringScheduler(DataGatheringScheduler scheduler)
    {
        _scheduler = scheduler;
    }

    @Required
    public void setSerialisers(Iterable<StateSerialiser> serialisers)
    {
        Map<String,StateSerialiser> available = new HashMap<>();

        for (StateSerialiser serialiser : serialisers) {
            available.put(serialiser.getName(), serialiser);
        }

        _availableSerialisers = available;

        if (_defaultSerialiser != null) {
            _currentSerialiser = _availableSerialisers.get(_defaultSerialiser);
        }
    }

    @Required
    public void setDefaultSerialiser(String name)
    {
        _defaultSerialiser = name;

        if (_availableSerialisers != null) {
            _currentSerialiser = _availableSerialisers.get(name);
        }
    }


    @Required
    public void setConduits(Iterable<Conduit> conduits)
    {
        _conduits = new HashMap<>();

        for (Conduit conduit : conduits) {
            _conduits.put(conduit.toString(), conduit);
        }
    }



	/**
	 * Switch on "enable" a named conduit.
	 * @param name the name of the conduit to enable.
	 * @return null if there was no problem, or a description of the problem otherwise.
	 */
	private String enableConduit( String name) {
		Conduit con = _conduits.get(name);

		if( con == null) {
                    return "Conduit " + name + " was not found.";
                }

		if( con.isEnabled()) {
                    return "Conduit " + name + " is already enabled.";
                }

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

		if( con == null) {
                    return "Conduit " + name + " was not found.";
                }

		if( !con.isEnabled()) {
                    return "Conduit " + name + " is not currently enabled.";
                }

		con.disable();

		return null;
	}



    @Required
    public void setMessageHandlerChain(MessageHandlerChain mhc)
    {
            _msgHandlerChain = mhc;
    }


    public synchronized InfoGetSerialisedDataMessage messageArrived(InfoGetSerialisedDataMessage message)
    {
        _log.trace("Received InfoGetSerialisedDataMessage.");

        StateSerialiser serialiser = _availableSerialisers.get(message.getSerialiser());

        if (serialiser == null) {
            _log.error("Couldn't find serialiser {}", message.getSerialiser());
            throw new IllegalArgumentException("no such serialiser");
        }

        String data;

        if (message.isCompleteDump()) {
            data = serialiser.serialise();
        } else {
            StatePath path = StatePath.buildFromList(message.getPathElements());
            data = serialiser.serialise(path);
        }

        message.setData(data);

        return message;
    }


	/**
	 *   H A N D L E R    A D M I N    C O M M A N D S
	 */

	public static final String fh_handler_ls = "List all known Message handlers.  These are responsible for updating dCache state.";
    public static final String hh_handler_ls = "";
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


	public static final String fh_conduits_ls = "List all known conduits.  Conduits provide read-only access to dCache current state.";
	public String ac_conduits_ls_$_0( Args args) {
		StringBuilder sb = new StringBuilder();

		sb.append("Conduits:\n");

		if( _conduits.size() > 0) {
                    for (Conduit con : _conduits.values()) {
                        sb.append(ADMIN_INTERFACE_LIST_PREFIX);
                        sb.append(con.toString());
                        sb.append("  ");
                        sb.append(con.getInfo());
                        sb.append("\n");
                    }
                } else {
			sb.append( ADMIN_INTERFACE_LIST_PREFIX);
			sb.append( ADMIN_INTERFACE_NONE);
			sb.append( "\n");
		}

		return sb.toString();
	}

	public static final String fh_conduits_enable = "Enabled the named conduit.";
	public static final String hh_conduits_enable = "<conduit name>";
	public String ac_conduits_enable_$_1( Args args) {
		String errMsg = enableConduit( args.argv(0));
		return errMsg == null ? ADMIN_INTERFACE_OK : errMsg;
	}

	public static final String fh_conduits_disable = "Disable the named conduit.";
	public static final String hh_conduits_disable = "<conduit name>";
	public String ac_conduits_disable_$_1( Args args) {
		String errMsg = disableConduit( args.argv(0));
		return errMsg == null ? ADMIN_INTERFACE_OK : errMsg;
	}


	/**
	 *   D G A   A D M I N   C O M M A N D S
	 */

	public static final String fh_dga_ls = "list all known data-gathering activity, whether enabled or not.";
	public String ac_dga_ls_$_0( Args args) {
		StringBuilder sb = new StringBuilder();
		sb.append( "Data-Gathering Activity:\n");
		List<String> dgaList = _scheduler.listActivity();

		if( dgaList.size() > 0) {
                    for (String activity : dgaList) {
                        sb.append(ADMIN_INTERFACE_LIST_PREFIX);
                        sb.append(activity);
                        sb.append("\n");
                    }
                } else {
			sb.append( ADMIN_INTERFACE_LIST_PREFIX);
			sb.append( ADMIN_INTERFACE_NONE);
			sb.append( "\n");
		}

		return sb.toString();
	}

	public static final String hh_dga_disable = "<name>";
	public static final String fh_dga_disable = "disable a data-gathering activity.";
	public String ac_dga_disable_$_1( Args args) {
		String errMsg = _scheduler.disableActivity(args.argv(0));
		return errMsg == null ? ADMIN_INTERFACE_OK : errMsg;
	}

	public static final String hh_dga_enable = "<name>";
	public static final String fh_dga_enable = "enable a data-gathering activity.  The next trigger time is randomly chosen.";
	public String ac_dga_enable_$_1( Args args) {
		String errMsg = _scheduler.enableActivity( args.argv(0));
		return errMsg == null ? ADMIN_INTERFACE_OK : errMsg;
	}

	public static final String hh_dga_trigger = "<name>";
	public static final String fh_dga_trigger = "trigger data-gathering activity <name> now.";
	public String ac_dga_trigger_$_1( Args args) {
		String errMsg = _scheduler.triggerActivity( args.argv(0));
		return errMsg == null ? ADMIN_INTERFACE_OK : errMsg;
	}


	/**
	 *   S T A T E   A D M I N   C O M M A N D S
	 */

	public static final String fh_state_ls = "List current status of dCache";
	public static final String hh_state_ls = "[<path>]";
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

		if( start != null) {
                    sb.append(_currentSerialiser.serialise(start));
                } else {
                    sb.append(_currentSerialiser.serialise());
                }

		if( sb.length() > 1) {
                    sb.append("\n");
                }

		return sb.toString();
	}

	public static final String fh_state_output = "view or change output format for the \"state ls\" command.";
	public static final String hh_state_output = "[<format>]";
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
			if( itr.hasNext()) {
                            sb.append(", ");
                        }
		}
		sb.append("\n");

		return sb.toString();
	}

	public static final String fh_state_pwd = "List the current directory for state ls";
	public String ac_state_pwd_$_0( Args args) {
		StringBuilder sb = new StringBuilder();

		if( _startSerialisingFrom != null) {
                    sb.append(_startSerialisingFrom.toString());
                } else {
                    sb.append(TOPLEVEL_DIRECTORY_LABEL);
                }

		return sb.toString();
	}

	public static final String fh_state_cd = "Change directory for state ls; path elements must be slash-separated";
	public static final String hh_state_cd = "<path>";
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
			if( path.startsWith("/")) {
                            currentPath = null; // cd is with absolute path, so reset our path.
                        }

			pathElements = path.split( "/");
		}

		/**
		 * As a special case: no slashes, single element in list containing dots that
		 * isn't "." or "..".  Treat this as a relative path, splitting on the dots.
		 */
		if( !quoted && pathElements.length == 1) {
			String element = pathElements [0];

			if( !element.contains( "/") && element.contains(".") && !element.equals(".") && !element.equals("..")) {
				if( currentPath != null) {
                                    currentPath = currentPath.newChild(StatePath
                                            .parsePath(element));
                                } else {
                                    currentPath = StatePath.parsePath(element);
                                }

				return currentPath;
			}
		}


		for( int i = 0; i < pathElements.length; i++) {

                    switch (pathElements[i]) {
                    case "..":
                        // Ascend once in the hierarchy.
                        if (currentPath != null) {
                            currentPath = currentPath.parentPath();
                        } else {
                            throw (new BadStatePathException("You cannot cd upward from the top-most element."));
                        }
                        break;
                    case ".":
                        // Do nothing, just to emulate Unix FS semantics.
                        break;
                    default:
                        if (pathElements[i].length() > 0) {
                            if (currentPath == null) {
                                currentPath = new StatePath(pathElements[i]);
                            } else {
                                currentPath = currentPath
                                        .newChild(pathElements[i]);
                            }
                        } else {
                            if (i == 0) {
                                currentPath = null; // ignore initial empty element from absolute paths
                            } else {
                                throw (new BadStatePathException("Path contains zero-length elements."));
                            }
                        }
                        break;
                    }
		}

		return currentPath;
	}


	/**
	 *  W A T C H E R    A D M I N    C O M M A N D S
	 */
	public static final String fh_watchers_ls = "list all registered dCache state watchers";
	public String ac_watchers_ls_$_0( Args args) {
		StringBuilder sb = new StringBuilder();
		sb.append( "State Watchers:\n");
		String watcherNames[] = _observatory.listStateWatcher();

		if( watcherNames.length > 0) {
                    for (String name : watcherNames) {
                        sb.append(ADMIN_INTERFACE_LIST_PREFIX);
                        sb.append(name);
                        sb.append("\n");
                    }
                } else {
			sb.append( ADMIN_INTERFACE_LIST_PREFIX);
			sb.append( ADMIN_INTERFACE_NONE);
			sb.append( "\n");
		}

		return sb.toString();
	}

	public static final String fh_watchers_enable = "enable a registered dCache state watcher";
	public String ac_watchers_enable_$_1( Args args) {
		int count;

		count = _observatory.enableStateWatcher(args.argv(0));

		switch( count) {
		case 0:
			return "No matching watcher: " + args.argv(0);
		case 1:
			return "Done.";
		default:
			return "Name matching multiple Watchers, all now enabled.";
		}
	}

	public static final String fh_watchers_disable = "disable a registered dCache state watcher";
	public String ac_watchers_disable_$_1( Args args) {
		int count;

		count = _observatory.disableStateWatcher(args.argv(0));

		switch( count) {
		case 0:
			return "No matching watcher: " + args.argv(0);
		case 1:
			return "Done.";
		default:
			return "Name matching multiple Watchers, all now disabled.";
		}
	}
}
