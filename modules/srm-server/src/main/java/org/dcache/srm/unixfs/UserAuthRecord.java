package org.dcache.srm.unixfs;

import java.util.Collection;

public class UserAuthRecord extends UserAuthBase
{
    public Collection<String> principals;

    public UserAuthRecord(String user,
			  boolean readOnly,
                          int uid,
                          int gid,
                          String home,
                          String root,
                          String fsroot,
                          Collection<String> principals)
    {
        super( user, readOnly, uid, gid, home, root,fsroot);
        this.principals = principals;
    }

    public UserAuthRecord()
    {
        super( null, true, -1, -1, "", "","");
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder(Username);
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
        if(principals != null)
        {
            for (Object principal : principals) {
                sb.append("  ").append(principal).append('\n');
            }
        }
        return sb.toString();
    }

    public String toDetailedString()
    {
        StringBuilder sb = new StringBuilder(" User Authentication Record for ");
        sb.append(Username).append(" :\n");
	sb.append("      read-only = ").append(readOnlyStr()).append("\n");
        sb.append("            UID = ").append(UID).append('\n');
        sb.append("            GID = ").append(GID).append('\n');
        sb.append("           Home = ").append(Home).append('\n');
        sb.append("           Root = ").append(Root).append('\n');
        sb.append("         FsRoot = ").append(FsRoot).append('\n');

        if(principals != null)
        {
            sb.append("         Secure Ids accepted by this user :\n");
            for (Object principal : principals) {
                sb.append("    SecureId  = \"").append(principal)
                        .append("\"\n");
            }
        }
        return sb.toString();
    }

    @Override
    public boolean isAnonymous() { return false; }
    @Override
    public boolean isWeak() {return false; }

    public boolean hasSecureIdentity(String p)
    {
      if(principals!=null)
      {
          return principals.contains(p);
      }
      return false;
    }

    public boolean isValid()
    {
        return Username != null;
    }

    public void addSecureIdentity(String id)
    {
        principals.add(id);
    }

    public void addSecureIdentities(Collection<String> ids)
    {
        // this will check that all elements in ids are Strings
        ids.toArray(new String[ids.size()]);
        principals.addAll(ids);
    }

    public void removeSecureIdentities(Collection<String> ids)
    {
        // this will check that all elements in ids are Strings
        ids.toArray(new String[ids.size()]);
        principals.removeAll(ids);
    }

}

