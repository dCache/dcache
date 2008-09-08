package dmg.cells.applets.alias ;


public class WasteRequest extends BaseRequest {
   private String _sname = "waste" ;
   private int    _a = 4 ;
   public String getSuperName(){ return _sname ; }
   public int    getTopSerial(){ return _a ; }
   public void   setTopSerial( int a , int b ){ _a = a ; }
   public void   setSuperName( String str ){ _sname = str ; }
   
   private int _A = 33 ;
   private long _B = 12345L ;
   private String _C = "xxx" ;
   
   public void setABC( int A , long B , String C ){
     _A = A ;
     _B = B ;
     _C = C ;
   }
   public int  getA(){ return _A ; }
   public long getB(){ return _B ; }
   public String getC(){ return _C ; }

}
