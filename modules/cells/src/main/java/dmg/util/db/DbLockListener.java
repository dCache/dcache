package dmg.util.db ;

public interface DbLockListener {

      public void readLockGranted() ; 
      public void writeLockGranted() ;
      public void readLockReleased() ;
      public void writeLockReleased() ;

}
