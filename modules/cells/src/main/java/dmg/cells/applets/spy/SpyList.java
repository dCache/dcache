package dmg.cells.applets.spy ;

import java.awt.* ;


public class SpyList extends List {


    private static final long serialVersionUID = -4907333829669036981L;

    public synchronized void select( String item ){
       int count = getItemCount() ;
       for( int i = 0 ; i < count ; i++ ) {
           if (getItem(i).equals(item)) {
               select(i);
               break;
           }
       }
       
    }
    public void deselectAll(){
        int [] sel = getSelectedIndexes() ;
        for (int index : sel) {
            deselect(index);
        }
    }

}
