/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.ftp;

import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Represents list of ranges of integers (ByteRange objects). 
   The name reflects the fact that in FTP extended mode restart markers, such
   structure represent a list of ranges of transfered bytes.
   The list has following characteristic:
   <ul>
   <li> no ranges from the list are adjacent nor have any common subset.
   In other words, for any two list members, r1.merge(r2) always
   returns ByteRange.THIS_ABOVE or ByteRange.THIS_BELOW
   <li> ranges in the list are ordered by the value of "from" field
   (or "to" field; it's the same)
   </ul>
   You cannot just add new ranges to the list, because that would violate
   the contract above. New ranges can be merge()d to the list.
   @see GridFTPRestartMarker 
 **/
public class ByteRangeList implements RestartData {

    private static Log logger = 
        LogFactory.getLog(ByteRangeList.class.getName());

    /**
       vector of ByteRanges. It is guaranteed 
       that any two ranges are not adjacent to each other,
       nor have a common subset.
       They are unordered, however.
     **/
    protected Vector vector;

    public ByteRangeList() {
	vector = new Vector();
    }
    
    /**
       @return true if this list logically represents the same range list,
       although the object instances may be different.
     **/
    public boolean equals(Object  other) {
        if (this == other) {
            return true;
        } 
        if (other instanceof ByteRangeList) {
            ByteRangeList otherObj = (ByteRangeList)other;
            if (this.vector.size() != otherObj.vector.size()) {
                return false;
            }
            for (int i=0; i<this.vector.size(); i++) {
                if (! this.vector.elementAt(i).equals(otherObj.vector.elementAt(i))) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public int hashCode() {
        int value = 0;
        for (int i=0; i<this.vector.size(); i++) {
            value += this.vector.elementAt(i).hashCode();
        }
        return value;
    }

    /**
       Merge a copy of the given ByteRange into this list.
       The resulting range list will represent
       all the integers represented so far, plus the integers represented
       by the new range.
       The resulting list will be stored in this object,
       while the parameter object will remain intact.
       For instance:
       <ul>
       <li>merge("10-15 30-35", "20-25") ->  "10-15 20-25 30-35"
       <li>merge("10-15 30-35", "12-15") ->  "10-15 20-25"
       <li>merge("10-15 30-35", "16-40") ->  "10-40"
       </ul>
     **/
    public void merge(final ByteRange range) {
	// always use copies of objects
	ByteRange newRange = new ByteRange(range);
	logger.debug( this.toFtpCmdArgument() + " + " + newRange.toString());
	int oldSize = vector.size();
	int index = 0;
	final int NOT_YET = -1;
	int merged = NOT_YET;

	if (oldSize == 0) {
	    vector.add(newRange);
	    return;
	}
	
	for (int i = 0; i < oldSize; i++) {

	    int result = newRange.merge((ByteRange)vector.elementAt(index));

	    switch (result) {
	    case ByteRange.THIS_ABOVE :
		//last_below = index;
		index ++;
		break; 
	    case ByteRange.ADJACENT :
	    case ByteRange.THIS_SUBSET :
	    case ByteRange.THIS_SUPERSET :
		if (merged == NOT_YET) {
		    vector.remove(index);
		    vector.add(index, newRange);
		    merged = index;
		    index++;
		} else {
		    vector.remove(index);
		    //do not augment index
		}
		break;
	    case ByteRange.THIS_BELOW :
		if (merged == NOT_YET) {
		    vector.add(index, newRange);
		}
		return;
	    }
	}
	
	if (merged == NOT_YET) {
	    vector.add(newRange);
	}
    }

    /**
       Merge into this list all the ranges contained 
       in the given vector using merge(ByteRange).
       @param other the Vector of ByteRange objects
     **/
    public void merge(final Vector other) {
	for(int i =0; i<other.size(); i++) {
	    this.merge((ByteRange)other.elementAt(i));
	}
    }


    /**
       Merge into this list all the ranges contained 
       in the given ByteRangeList using merge(ByteRange).
       The parameter object remains intact.
       @param other the ByteRangeList to be merged into this 
     **/
    public void merge(final ByteRangeList other) {
	merge(other.vector);
    }

    /**
       convert this object to a vector of ByteRanges.
       The resulting vector will preserve the features
       of ByteRangeList: (1) order and (2) separation.
       Subsequent calls of this method will return
       the same Vector object.
     **/
    public Vector toVector() {
	return vector;
    }

    /**
       convert this object to a String, in the format
       of argument of REST GridFTP command, for instance:
       "0-29,32-89"
       The resulting String will preserve the features
       of ByteRangeList: (1) order and (2) separation
     **/
    public String toFtpCmdArgument() {
	char comma = ',';
	boolean first = true;
	StringBuffer result = new StringBuffer();
	for (int i = 0; i < vector.size(); i++) {

	    if (first) {
		first = false;
	    } else {
		result.append(comma);
	    }

	    result.append(vector.elementAt(i).toString());

	}
	return result.toString();
    }
}
