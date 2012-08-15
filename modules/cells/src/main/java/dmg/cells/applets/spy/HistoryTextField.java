package dmg.cells.applets.spy ;

import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;

public class HistoryTextField 
       extends TextField
       implements KeyListener , ActionListener   {

   private static Vector __history = new Vector() ;
   private static final long serialVersionUID = 3682870067347991467L;
   private ActionListener _listener;
   private int _position;
   public HistoryTextField(){ 
      super() ; 
      addKeyListener( this ) ;
      super.addActionListener( this ) ;
   }
   @Override
   public void addActionListener( ActionListener listener ){
       _listener = listener ;
   }
   @Override
   public void keyPressed( KeyEvent event ){
       if( event.getKeyCode() == KeyEvent.VK_UP ){
          if( _position < __history.size() ) {
              setText((String) __history.elementAt(_position++));
          }
             
       }else if( event.getKeyCode() == KeyEvent.VK_DOWN ){
          if( _position > 0 ) {
              setText((String) __history.elementAt(--_position));
          } else if( _position == 0 ) {
              setText("");
          }
       }
   }
   @Override
   public void keyReleased( KeyEvent event ){}
   @Override
   public void keyTyped( KeyEvent event ){}
   @Override
   public void actionPerformed( ActionEvent event ){
        String command = getText() ;
        if(  ( ! command.equals("") ) &&
             ( ( __history.size() == 0 ) || 
               ! __history.elementAt(0).equals(command) ) ) {
            __history.insertElementAt(getText(), 0);
        }
           
        if( _listener != null ) {
            _listener.actionPerformed(event);
        }
        _position = 0 ;
   }

}
