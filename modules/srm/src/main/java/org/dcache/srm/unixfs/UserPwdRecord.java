package org.dcache.srm.unixfs;

public class UserPwdRecord extends UserAuthBase
{
    String Password;

	public UserPwdRecord(String user,
			     String passwd, boolean readOnly,
                         int uid,int gid,
                         String home,String root,String fsroot)
	{
        this(user,passwd,readOnly, uid, gid, home, root, fsroot,false);
	}
    
    public UserPwdRecord(String user,
                         String passwd, boolean readOnly, 
                         int uid,int gid,
                         String home,String root,String fsroot,
                         boolean isPlain)
    {
        super(user, readOnly, uid, gid, home, root, fsroot);
        
        if(isPlain)
        {
            setPassword(passwd);
        }
        else
        {
            Password = passwd;
        }
    }

    @Override
    public boolean isWeak() { return true; }


 	public String serialize()
 	{
	    
 		String str = Username + " " +
 			Password + " " +
 		    readOnlyStr() + " " + 
 			UID + " " +
 			GID + " " +
 			Home + " " +
 			Root;
 		if ( ! Root.equals(FsRoot) ) {
                     str = str + " " + FsRoot;
                 }
 		return str;
 	}

// 	public void deserialize(String line)
// 	{
// 		StringTokenizer t = new StringTokenizer(line);
// 		int ntokens = t.countTokens();
// 		Username = null;
// 		if ( ntokens < 6 )
// 			return;
// 		Username = t.nextToken();
// 		Password = t.nextToken();
// 		UID = Integer.parseInt(t.nextToken());
// 		GID = Integer.parseInt(t.nextToken());
// 		Home = t.nextToken();
// 		Root = t.nextToken();
// 		FsRoot = new String(Root);
// 		if( ntokens > 6 )
// 			FsRoot = t.nextToken();
// 	}

   public String toString()
    {
        return serialize();
    }

    public String toDetailedString()
    {
        StringBuilder stringbuffer = new StringBuilder(" User Password Record for ");
        stringbuffer.append(Username).append(" :\n");
        stringbuffer.append("  Password Hash = ").append(Password).append('\n');
	stringbuffer.append("      read-only = ").append(readOnlyStr())
                .append("\n");
        stringbuffer.append("            UID = ").append(UID).append('\n');
        stringbuffer.append("            GID = ").append(GID).append('\n');
        stringbuffer.append("           Home = ").append(Home).append('\n');
        stringbuffer.append("           Root = ").append(Root).append('\n');
        stringbuffer.append("         FsRoot = ").append(FsRoot).append('\n');
        return stringbuffer.toString();
    }

	
	public String hashPassword(String pwd)
	{
		String uandp = "1234567890" + Username + " " + pwd;
		return Integer.toHexString(uandp.hashCode());
	}

	public void setPassword(String pwd)
	{
		if( pwd.equals("-") ) {
                    Password = "-";
                } else {
                    Password = hashPassword(pwd);
                }
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
	
	@Override
        public boolean isAnonymous()
	{
		return Password.equals("-");
	}

	public boolean isValid()
	{
		return Username != null &&
			Password != null;
	}
}

