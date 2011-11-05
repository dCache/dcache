package gplazma.authz.records;

public class PasswordRecord extends AuthorizationRecordBase
{
    String Password = null;

	public PasswordRecord(String user,
					     String passwd, boolean readOnly,
                         int priority, int uid,int[] gids,
                         String home,String root,String fsroot)
	{
        this(user, passwd, readOnly, priority, uid, gids, home, root, fsroot,false);
	}

    public PasswordRecord(String user,
                         String passwd, boolean readOnly,
                         int priority, int uid,int[] gids,
                         String home,String root,String fsroot,
                         boolean isPlain)
    {
        super(user, readOnly, priority, uid, gids, home, root, fsroot);

        if(isPlain)
        {
            setPassword(passwd);
        }
        else
        {
            Password = passwd;
        }
    }


    public String serialize()
    {

 		String str = getUsername() + " " +
 			Password + " " +
 		  readOnlyStr() + " " +
 			getPriority() + " " +
      getUID() + " " +
 			getGIDs() + " " +
 			getHome() + " " +
 			getRoot();
 		if ( ! getRoot().equals(getFsRoot()) )
 			str = str + " " + getFsRoot();
 		return str;
 	}

    @Override
    public String toString()
    {
        return serialize();
    }

    public String toDetailedString()
    {
        StringBuffer stringbuffer = new StringBuffer(" User Password Record for ");
        stringbuffer.append(getUsername()).append(" :\n");
        stringbuffer.append("  Password Hash = ").append(Password).append('\n');
		    stringbuffer.append("      read-only = " + readOnlyStr() + "\n");
        stringbuffer.append("       priority = ").append(getPriority()).append('\n');
        stringbuffer.append("            UID = ").append(getUID()).append('\n');
        stringbuffer.append("            GID = ").append(getGIDs()).append('\n');
        stringbuffer.append("           Home = ").append(getHome()).append('\n');
        stringbuffer.append("           Root = ").append(getRoot()).append('\n');
        stringbuffer.append("         FsRoot = ").append(getFsRoot()).append('\n');
        return stringbuffer.toString();
    }


	public String hashPassword(String pwd)
	{
		String uandp = "1234567890" + getUsername() + " " + pwd;
		return java.lang.Integer.toHexString(uandp.hashCode());
	}

	public void setPassword(String pwd)
	{
		if( pwd.equals("-") )
			Password = "-";
		else
			Password = hashPassword(pwd);
	}

	public void disable()
	{
		Password = "#";
	}

	public boolean passwordIsValid(String clear_pwd)
	{
		return Password.equals(hashPassword(clear_pwd));
	}

	public boolean isDisabled()
	{
		return Password.equals("#");
	}

	public boolean isAnonymous()
	{
		return Password.equals("-");
	}

        public boolean isWeak() { return true; }

	public boolean isValid()
	{
		return getUsername() != null &&
			Password != null;
	}
}

