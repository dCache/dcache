package dmg.util ;
import java.awt.* ;
import java.awt.event.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      jWorksheet 
       extends    TextArea 
       implements KeyListener , TextListener{

   private static final long serialVersionUID = -8961732278625042991L;
   ActionListener _actionListener;
   String         _actionCommand   = "" ;
   String         _selectionText;
   int            _answerStart = -1 , 
                  _answerEnd   = -1 ,
                  _changedCounter;
   final Object         _lock = new Object() ;
   
   public jWorksheet(){
        super( 24 , 80 ) ;
        addKeyListener( this ) ;
        addTextListener( this ) ;
   } 
   public jWorksheet( int rows , int columns ){
        super( rows , columns ) ;
        addKeyListener( this ) ;
        addTextListener( this ) ;
   } 
   public void setActionListener( ActionListener listener ){
      _actionListener = listener ;
   }
   public void setActionCommand( String com ){
      _actionCommand = com ;
   }
   @Override
   public void textValueChanged( TextEvent event ){
//      say( " text event : "+_changedCounter+" : "+event ) ;
//      say( " text event : "+event.paramString() ) ;
//     say( " text event at caret : "+getCaretPosition() ) ;
      synchronized( _lock ){ 
          if( _changedCounter--  > 0 ) {
              return;
          }
      }
      _answerStart = -1 ;
   }
   @Override
   public void keyPressed( KeyEvent event ){
//     say( "event : "+event);
     if( ( event.getKeyCode() == KeyEvent.VK_ENTER ) &&
           event.isShiftDown()                           ){
           
         String x;
         try{  x = getSelectedText() ;}
         catch( Exception i ){ x = null ; }
         
//         say( x ) ;
         if( ( x == null ) ||   x.equals("") ){
            x = _getCurrentLine2() ;
         }
         event.consume();
         if(_actionListener!=null) {
             _actionListener.actionPerformed(
                     new ActionEvent(this, 0, x));
         }
       
     }else if( event.isControlDown() && ( event.getKeyCode() == 90 ) ){
       // cntr  z
        event.consume();
        if( _answerStart >= 0 ){
           select(  _answerStart , _answerEnd ) ;
           String x = getSelectedText() ;
           if( ( x != null ) && ! x.equals("") ) {
               _selectionText = x;
           }
           replaceRange( "" , getSelectionStart() , getSelectionEnd() ) ;
        }
     }else if( event.isControlDown() && ( event.getKeyCode() == 67 ) ){
       // cntr  c
        event.consume();
        String x = getSelectedText() ;
        if( ( x != null ) && ! x.equals("") ) {
            _selectionText = x;
        }
     }else if( event.isControlDown() && ( event.getKeyCode() == 86 ) ){
       // cntr  v
        event.consume();
        if( _selectionText != null ) {
            insert(_selectionText, getCaretPosition());
        }
     }else if( event.isControlDown() && ( event.getKeyCode() == 88 ) ){
       // cntr  x
        event.consume();
        String x = getSelectedText() ;
        if( ( x != null ) && ! x.equals("") ) {
            _selectionText = x;
        }
        replaceRange( "" , getSelectionStart() , getSelectionEnd() ) ;
     }else{
//        say( " key pressed at caret : "+getCaretPosition() ) ;
     }
   }
   public void answer( String a ){
     int c = getCaretPosition() ;
     synchronized( _lock ){ _changedCounter = 2 ; }
     insert( "\n"+a , c ) ;
//     say( "last insert finished" ) ;
     _answerStart = c ;
     _answerEnd   = getCaretPosition() ;
//     insert( "\n" , getCaretPosition() ) ;
   }
   @Override
   public void keyTyped( KeyEvent event ){ }
   @Override
   public void keyReleased( KeyEvent event ){ }
   
   private String _getCurrentLine(){
     int c = getCaretPosition() ;
     int cc = c - 100 ;
     int x0 = cc < 0 ? 0 : cc ;
     int x1 = c + 100 ;
     select(x0,x1) ;
     String x = getSelectedText() ;
     if( x.length() == 0 ) {
         return "";
     }
     int f = c - x0 ;
     int i  ;
     for( i = f - 1 ; ( i >= 0 ) && ( x.charAt(i)!='\n' ) ; i-- ) {
     }
     int xMin = x0 + i + 1 ;
     for( i = f ; ( i < x.length() ) && ( x.charAt(i)!='\n' ) ; i++ ) {
     }
     int xMax = x0 + i  ;
     select(xMin,xMax) ;
     return  getSelectedText() ;
 
   }
   private String _getCurrentLine2(){
        int    c = getCaretPosition() ;
        String x = getText() ;
        if( x.length() == 0 ) {
            return "";
        }
        int i  ;
        for( i = c - 1 ; ( i >= 0 ) && ( x.charAt(i)!='\n' ) ; i-- ) {
        }
        int xMin = i + 1 ;
        for( i = c ; ( i < x.length() ) && ( x.charAt(i)!='\n' ) ; i++ ) {
        }
        int xMax = i  ;
        select(xMin,xMax) ;
        x = getSelectedText() ;
        c = getCaretPosition() ;
        select(c,c) ;
        return x ;
 
   }
        
   private void say( String text ){  System.out.println( text ) ; }
        
}
                               
