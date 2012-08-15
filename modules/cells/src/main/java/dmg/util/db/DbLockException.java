package dmg.util.db ;

public class DbLockException extends DbException {

    private static final long serialVersionUID = 9080130956338408514L;

    public DbLockException( String msg ){ super( msg  ) ; }
}
