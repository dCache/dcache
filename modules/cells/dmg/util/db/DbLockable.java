package dmg.util.db ;

public interface DbLockable {
   public void open( int flags ) throws DbLockException, InterruptedException ;
   public void close() throws DbLockException ;
}
