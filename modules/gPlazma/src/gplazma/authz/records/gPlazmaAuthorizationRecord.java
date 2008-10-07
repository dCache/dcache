package gplazma.authz.records;

import java.io.Serializable;

public class gPlazmaAuthorizationRecord extends AuthorizationRecordBase implements Serializable
{
    private static final long serialVersionUID = 4793270902707803837L;
    private String SubjectDN;
    private String FQAN;
    private Object custom_record;
    private long authRequestID;

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

    public gPlazmaAuthorizationRecord(String user,
                                      boolean readOnly,
                                      int priority,
                                      int uid,
                                      int[] gids,
                                      String home,
                                      String root,
                                      String fsroot,
                                      String dn,
                                      String fqan,
                                      long authRequestID)
    {
        super( user, readOnly, priority, uid, gids, home, root,fsroot);
        this.SubjectDN=dn;
        this.FQAN=fqan;
        this.authRequestID=authRequestID;
    }

    public gPlazmaAuthorizationRecord(Object obj)
    {
        this( null, true, 0, -1, null, "", "", "");
        this.custom_record=obj;
    }

    public gPlazmaAuthorizationRecord()
    {
    }

    public String getSubjectDN() {
        return SubjectDN;
    }

    public void setSubjectDN(String dn) {
        SubjectDN=dn;
    }

    public String getFqan() {
        return FQAN;
    }

    public void setFqan(String fqan) {
        FQAN=fqan;
    }

    public Object getCustomRecord() {
        return custom_record;
    }

    public long getRequestID() {
        return authRequestID;
    }

    public void setRequestID(long req_id) {
        authRequestID=req_id;
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

    public boolean isValid()
    {
        return getUsername() != null;
    }

    public boolean isAnonymous() { return false; }
    public boolean isWeak() {return false; }

}

