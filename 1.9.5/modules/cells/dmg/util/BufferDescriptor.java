package dmg.util ;

//
// is 
public class BufferDescriptor {
   private byte [] _buffer ;
   private int     _mode ;
   private int     _usable ;
   private int     _size ;
   private int     _num ;
   private int     _usageCounter = 0 ;
   
   public static final int FILLED   = 0 ;
   public static final int EMPTY    = 1 ;
   public static final int FILLING  = 2 ;
   public static final int DRAINING = 3 ;
   private static final String [] _modeStrings = 
     { "Filled" , "Empty" , "Filling" , "Draining" } ;
   //
   // constructor can only be called by this package
   //
   BufferDescriptor( int size , int bufferNumber ){
      _buffer = new byte [_size=size] ;
      _mode   = EMPTY ;   
      _num    = bufferNumber ;
      _usable = 0 ; 
   }
   public void setFilled(){ _mode = FILLED ; }
   public void setEmpty(){  _mode = EMPTY  ; }
   public void setFilling(){ _mode = FILLING ; }
   public void setDraining(){ _mode = DRAINING ; }
   void setMode( int mode ){ _mode = mode ; }
   int  getMode(){ return _mode ; }
   public byte [] getBase(){ return _buffer ; }
   public int getSize(){ return _size ; }
   public int getUsable(){  return _usable ; }
   public void setUsable(int usable ){ _usageCounter++ ; _usable = usable ; }
   public String modeToString(){ return _modeStrings[_mode] ; }
   public String toString(){
      return "BufferDescriptor n="+_num+
             ";u="+_usageCounter+
             ";m="+_modeStrings[_mode] ;
   }   

}
