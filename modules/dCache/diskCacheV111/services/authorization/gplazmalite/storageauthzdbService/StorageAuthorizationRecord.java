//package diskCacheV111.services.authorization.gplazmalite.storageauthzdbService;
package gplazma.gplazmalite.storageauthzdbService;

import java.util.*;
import java.io.*;

public class StorageAuthorizationRecord extends StorageAuthorizationBase
{

    public StorageAuthorizationRecord(String user,
			  boolean readOnly,
                          int uid,
                          int gid,
                          String home,
                          String root,
                          String fsroot)
    {
        super( user, readOnly, uid, gid, home, root,fsroot);
    }

    public StorageAuthorizationRecord()
    {
        super( null, true, -1, -1, "", "","");
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
        sb.append( ' ').append( UID).append( ' ');
        sb.append( GID ).append(' ');
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
        sb.append("            UID = ").append(UID).append('\n');
        sb.append("            GID = ").append(GID).append('\n');
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

