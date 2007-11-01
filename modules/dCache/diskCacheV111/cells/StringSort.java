// $Id: StringSort.java,v 1.2 2000-10-30 16:20:36 cvs Exp $

package diskCacheV111.cells;  //XXX move this to util

import java.util.*;

/** straightforward QuickSort.  Could use built-in sorting in Java1.2 */
public class StringSort{

    public static String[] sortStrings(String strings[]){
	quickSort(0, strings.length-1, strings);
	return strings;
    }
    private static String[] quickSort(int left, int right, String strings[]){
	if (right>left){
	    String s1 = strings[right];
	    int i = left - 1;
	    int j = right;
	    while (true){
		while (s1.compareTo(strings[++i]) > 0)
		    ;
		while (j>0)
		    if (s1.compareTo(strings[--j]) >= 0)
			break;
		if (i >= j) break;
		String t = strings[i];
		strings[i] = strings[j];
		strings[j] = t;
	    }
	    String t = strings[i];
	    strings[i] = strings[right];
	    strings[right] = t;
	    strings=quickSort(left, i-1, strings);
	    strings=quickSort(i+1, right, strings);
	}
	return strings;
    }
}

