package dmg.cells.applets ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;

public class SshClientActionPanel extends Panel {
     private ActionListener _actionListener ;
     
     public void addActionListener( ActionListener listener ){
       _actionListener = listener ;
     }
     public void informActionListeners(String msg ){
        if( _actionListener != null )
           _actionListener.actionPerformed( 
                new ActionEvent( this, 0,msg)      ) ;
     }



}
