//
// $Id: CVWritePacket.java,v 1.1 2002-03-18 09:04:44 cvs Exp $
//
package dmg.cells.services.codeVersioning ;


public class CVWritePacket extends CVContainerPacket {
   private static final long serialVersionUID = 3990453589754121884L;
   private long    _offset = -1 ;
   private byte [] _data;
   private int     _size;
   public CVWritePacket( String name , String type , byte [] data ){
      super(name,type);
      setData(data);
   }
   public CVWritePacket( String name , String type , byte [] data , int size ){
      super(name,type);
      setData(0,data,size);
   }
   public CVWritePacket( String name , String type ){
      super(name,type) ;
   }
   public void setData( byte [] data ){
      setData( 0 , data , data.length ) ;
   }
   public void setData( int  offset , byte [] data , int size ){
      _data   = new byte[_size = size] ;
      System.arraycopy( data , offset , _data , 0 , _size ) ;
      
   }
   public boolean isAppend(){ return _offset < 0 ; }
   public void setContainerOffset( long offset ){
      _offset = offset ;
   }
   public long getOffset(){ return _offset ; }
   public int getSize(){ return _size ; }
   public byte [] getData(){ return _data ; }
}
