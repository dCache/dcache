package org.dcache.auth;

public class UserPwdRecord extends UserAuthBase {
    private static final long serialVersionUID = 1335892861480300575L;

    String Password;

    public UserPwdRecord(String user,
                         String passwd,
                         boolean readOnly,
                         int uid,
                         int[] gid,
                         String home,
                         String root,
                         String fsroot) {
        this(user,passwd,readOnly, uid, gid, home, root, fsroot,false);
    }

    public UserPwdRecord(String user,
                         String passwd,
                         boolean readOnly,
                         int uid,
                         int[] gid,
                         String home,
                         String root,
                         String fsroot,
                         boolean isPlain) {
        super(user, readOnly, uid, gid, home, root, fsroot);
        if(isPlain) {
            setPassword(passwd);
        } else {
            Password = passwd;
        }
    }

	public UserPwdRecord(String user,
	                     String passwd,
	                     boolean readOnly,
                         int uid,
                         int gid,
                         String home,
                         String root,
                         String fsroot) {
        this(user,passwd,readOnly, uid, gid, home, root, fsroot,false);
	}

    public UserPwdRecord(String user,
                         String passwd,
                         boolean readOnly,
                         int uid,
                         int gid,
                         String home,
                         String root,
                         String fsroot,
                         boolean isPlain) {
        super(user, readOnly, uid, gid, home, root, fsroot);

        if(isPlain) {
            setPassword(passwd);
        } else {
            Password = passwd;
        }
    }

    @Override
    public boolean isWeak() { return true; }

    @Override
    public String toString() {
        String str = Username + ' ' +
                     readOnlyStr() + ' ' +
                     UID + ' ' +
                     GIDs + ' ' +
                     Home + ' ' +
                     Root;
        if ( ! Root.equals(FsRoot) ) {
            str = str + ' ' + FsRoot;
        }
        return str;
    }

    public String toDetailedString() {
        String stringbuffer = " User Password Record for " + Username + " :\n" +
                              "  Password Hash = " + Password + '\n' +
                              "      read-only = " + readOnlyStr() +
                              '\n' +
                              "            UID = " + UID + '\n' +
                              "           GIDs = " + GIDs + '\n' +
                              "           Home = " + Home + '\n' +
                              "           Root = " + Root + '\n' +
                              "         FsRoot = " + FsRoot + '\n';
        return stringbuffer;
    }


	public String hashPassword(String pwd) {
		String uandp = "1234567890" + Username + ' ' + pwd;
		return Integer.toHexString(uandp.hashCode());
	}

    public void setPassword(String pwd) {
        if (pwd.equals("-")) {
            Password = "-";
        } else {
            Password = hashPassword(pwd);
        }
    }

	public void disable() {
		Password = "#";
	}

	public boolean passwordIsValid(String clear_pwd) {
		return Password.equals(hashPassword(clear_pwd));
	}

	public boolean isDisabled() {
		return Password.equals("#");
	}

	@Override
    public boolean isAnonymous() {
		return Password.equals("-");
	}

	public boolean isValid() {
		return Username != null &&
			Password != null;
	}
}

