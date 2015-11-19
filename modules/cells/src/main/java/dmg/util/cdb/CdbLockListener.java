package dmg.util.cdb ;

public interface CdbLockListener {

      void readLockGranted() ;
      void writeLockGranted() ;
      void readLockReleased() ;
      void writeLockReleased() ;
      void writeLockAborted() ;

}
