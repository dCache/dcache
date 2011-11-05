package gplazma.authz.records;

import java.io.Serializable;
import java.util.Arrays;

public class gPlazmaAuthorizationRecord extends AuthorizationRecordBase implements Serializable
{
    private static final long serialVersionUID = 4793270902707803837L;
    private Object custom_record;

    public gPlazmaAuthorizationRecord(String user,
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

    public gPlazmaAuthorizationRecord(Object obj)
    {
        this( null, true, 0, -1, null, "", "", "");
        this.custom_record=obj;
    }

    public gPlazmaAuthorizationRecord()
    {
    }


    public Object getCustomRecord() {
        return custom_record;
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer(getUsername());
        if(isReadOnly()) {
            sb.append(" read-only");
        }
        else {
            sb.append(" read-write");
        }
        sb.append( ' ').append(getPriority()).append( ' ');
        sb.append( getUID() ).append(' ');
        for(int gid : getGIDs()){sb.append( gid ).append(' ');}
        sb.append( getHome() ).append(' ');
        sb.append( getRoot() ).append(' ');
        sb.append( getFsRoot() ).append('\n');
        return sb.toString();
    }

    public String toDetailedString()
    {
        StringBuffer sb = new StringBuffer(" User Authentication Record for ");
        sb.append(getUsername()).append(" :\n");
        sb.append("      read-only = ").append(readOnlyStr()).append("\n");
        sb.append("       priority = ").append(getPriority()).append('\n');
        sb.append("            UID = ").append(getUID()).append('\n');
        sb.append("            GIDs = ").append(getGIDs());
        sb.append("           Home = ").append(getHome()).append('\n');
        sb.append("           Root = ").append(getRoot()).append('\n');
        sb.append("         FsRoot = ").append(getFsRoot()).append('\n');

        return sb.toString();
    }

    public String toShortString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(getUsername()).append(" ").
                append(getUID()).append(" ").
                append(Arrays.toString(getGIDs())).append(" ").
                append(getRoot());

        return sb.toString();
    }

    public boolean isValid()
    {
        return getUsername() != null;
    }

    public boolean isAnonymous() { return false; }
    public boolean isWeak() {return false; }

}

