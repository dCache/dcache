package dmg.util.cdb ;

public interface CdbLockListener {

      public void readLockGranted() ; 
      public void writeLockGranted() ;
      public void readLockReleased() ;
      public void writeLockReleased() ;
      public void writeLockAborted() ;

}
