// $Id: SortedList.java,v 1.1 2000-01-08 20:45:59 cvs Exp $

package diskCacheV111.util;

import java.util.*;

public class SortedList {

    private Vector _scores;
    private Vector _objects;

    public SortedList(){
	_scores = new Vector();
	_objects = new Vector();
    }

    public boolean put(Object object, Comparable score){
	//returns true if object was not already in list
	
	if (_objects.contains(object)){
	    int i = _objects.indexOf(object);
	    if (((Comparable)_scores.elementAt(i)).compareTo(score) == 0){
		//nothing to do
		return false;
	    } else {
		//priority is changing, handle this by removing and reinserting
		_scores.removeElementAt(i);
		_objects.removeElementAt(i);
	    }
	}
	
	int insertAt=0;
	int size = _objects.size();
	if (size > 0){
	    if (score.compareTo((Comparable)_scores.elementAt(size-1))>=0){
		insertAt = size;
	    } else {
		int bottom=0;
		int top=_objects.size();
		while (top-bottom>1){
		    int middle = (top+bottom)/2;
		    Comparable middleScore = ((Comparable)_scores.elementAt(middle));
		    if (middleScore.compareTo(score) <= 0)
			bottom=middle;
		    if (middleScore.compareTo(score) >= 0)
			top=middle;
		}
		insertAt = top;
	    }
	}
	_objects.insertElementAt(object,insertAt);
	_scores.insertElementAt(score, insertAt);
	return true;
    }
    
    public boolean remove(Object object){
	//returns true if object was in list
	if (_objects.contains(object)){
	    int i = _objects.indexOf(object);
	    _scores.removeElementAt(i);
	    _objects.removeElementAt(i);
	    return true;
	}
	return false;
    }

    public Object getLowest(){
	int size = _objects.size();
	if (size==0){
	    return null;
	} else {
	    return _objects.elementAt(0);
	}
    }

    public Object getHighest(){
	int size = _objects.size();
	if (size==0){
	    return null;
	} else {
	    return _objects.elementAt(size-1);
	}
	
    }

    public int size(){
	return _objects.size();
    }
}
