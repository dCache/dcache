package dmg.cells.applets.spy ;

import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;

public class HistoryTextField 
       extends TextField
       implements KeyListener , ActionListener   {

   private static Vector __history = new Vector() ;
   private ActionListener _listener = null ;
   private int _position = 0 ;
   public HistoryTextField(){ 
      super() ; 
      addKeyListener( this ) ;
      super.addActionListener( this ) ;
   }
   public void addActionListener( ActionListener listener ){
       _listener = listener ;
   }
   public void keyPressed( KeyEvent event ){ 
       if( event.getKeyCode() == KeyEvent.VK_UP ){
          if( _position < __history.size() )
             setText( (String)__history.elementAt(_position++) ) ;
             
       }else if( event.getKeyCode() == KeyEvent.VK_DOWN ){
          if( _position > 0 )
             setText( (String)__history.elementAt(--_position) ) ;
          else if( _position == 0 )
             setText( "" ) ;
       }
   }
   public void keyReleased( KeyEvent event ){}
   public void keyTyped( KeyEvent event ){}
   public void actionPerformed( ActionEvent event ){
        String command = getText() ;
        if(  ( ! command.equals("") ) &&
             ( ( __history.size() == 0 ) || 
               ! __history.elementAt(0).equals(command) ) )
           __history.insertElementAt( getText() , 0  ) ;
           
        if( _listener != null )_listener.actionPerformed( event ) ;
        _position = 0 ;
   }

}
