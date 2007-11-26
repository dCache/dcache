package dmg.apps.libraryServer ;

import java.awt.* ;
import java.awt.event.* ;

public class      MulticastPanel  
       extends SimpleBorderPanel {
       
      private ActionListener _actionListener = null ;
      protected MulticastPanel(){}
      MulticastPanel( LayoutManager lm , int border , Color color ){
          super( lm , border , color ) ;
      }
      public void addActionListener( ActionListener actionListener ){
         _actionListener = AWTEventMulticaster.add(
                              _actionListener, actionListener ) ;
      }
      protected void processActionEvent( ActionEvent e ){
          if( _actionListener != null )
             _actionListener.actionPerformed(e) ;
      }
}
