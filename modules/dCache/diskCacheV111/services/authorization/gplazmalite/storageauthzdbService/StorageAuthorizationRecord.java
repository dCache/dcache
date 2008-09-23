//package diskCacheV111.services.authorization.gplazmalite.storageauthzdbService;
package gplazma.gplazmalite.storageauthzdbService;

import java.util.*;
import java.io.*;

public class StorageAuthorizationRecord extends StorageAuthorizationBase
{

    public StorageAuthorizationRecord(String user,
			                    boolean readOnly,
                          int priority,
                          int uid,
                          int[] gids,
                          String home,
                          String root,
                          String fsroot)
    {
        super( user, readOnly, priority, uid, gids, home, root,fsroot);
    }

    public StorageAuthorizationRecord()
    {
        super( null, true, 0, -1, new int[]{-1}, "", "", "");
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer(Username);
	if(ReadOnly) {
	    sb.append(" read-only");
	}
	else {
	    sb.append(" read-write");
	}
        sb.append( ' ').append(priority).append( ' ');
        sb.append( UID ).append(' ');
        for(int gid : GIDs){sb.append( gid ).append(' ');}
        sb.append( Home ).append(' ');
        sb.append( Root ).append(' ');
        sb.append( FsRoot ).append('\n');
        return sb.toString();
    }

    public String toDetailedString()
    {
        StringBuffer sb = new StringBuffer(" User Authentication Record for ");
        sb.append(Username).append(" :\n");
	      sb.append("      read-only = " + readOnlyStr() + "\n");
        sb.append("       priority = ").append(priority).append('\n');
        sb.append("            UID = ").append(UID).append('\n');
        sb.append("            GIDs = ").append(GIDs);
        sb.append("           Home = ").append(Home).append('\n');
        sb.append("           Root = ").append(Root).append('\n');
        sb.append("         FsRoot = ").append(FsRoot).append('\n');
        
        return sb.toString();
    }

    public boolean isValid()
    {
        return Username != null;
    }

    public boolean isAnonymous() { return false; }
    public boolean isWeak() {return false; }
    	
}

