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
package org.globus.common;

import java.util.Hashtable;
import java.io.Serializable;

/** ConstrainedMVHashtable allows the placing of constraints on a MVHashtable that
 * determine if an attribute is optional, required, or if an attribute is single, or
 * multivalued.  Use set/isRequired() functions to specify/check whether an
 * attribute will be required or not.  Use set/isSingleValued() to specify/check
 * whether an attribute accepts a single or multiple values.  Use setMaximum() to
 * set the largest number of values an attribute is allowed to accept.
 * NOTE: this class is not supported yet.
 * 
 */
public class ConstrainedMVHashtable extends MVHashtable implements Serializable {

  /** holds single-valued status of attributes **/
  protected Hashtable singleValued;

  /** holds required status of attributes **/
  protected Hashtable required;

  /** holds the maximum values for various attributes  */
  protected Hashtable maxValues;
    
  /** Default constructor which creates a blank ConstrainedMVHashtable.
   * 
   */
  public ConstrainedMVHashtable() {
    super();
  }

  /** Copy constructor.
   * 
   * @param constrainedMultivaluedHashtable the ConstrainedMVHashtable to be copied
   */
  public ConstrainedMVHashtable(ConstrainedMVHashtable constrainedMultivaluedHashtable) {
    super(constrainedMultivaluedHashtable);
  }

  /**********************************************************************/
  /** methods for required and optional attributes **/
  /**********************************************************************/

  /** Constrains a specific attribute to be required or optional. Each
   * attribute is by default optional.
   * 
   * @param key  the key of the attribute to be constrained
   * @param req  true if required, false if optional
   */
  public void setRequired(String key, boolean req) {
  }

  /** Constrains all attributes specified in the array of key strings to be
   * required/not required.
   * 
   * @param keys  an array of the keys to be constrained
   * @param required  true if all are required, false if all are optional
   */
  public void setRequired(String [] keys, boolean required) {
  }


  /** Returns true if the key is constrained to be required.
   *
   * @param key  the key of attribute to check
   *
   * @return <code>boolean</code> true if required, false if optional
   */
  public boolean isRequired(String key) {
    return false;
  }

  /**********************************************************************/
  /** methods for single or multivalued attributes **/
  /**********************************************************************/

  /** Constrains a specific attribute to be SingleValued. Each
   * attribute is by default multiValued.
   * 
   * @param key  the key of the attribute to be constrained
   * @param single  true if single-valued, false if multi-valued
   */
  public void setSingleValued(String key, boolean single) {
  }

  /** Constrains all attributes specified by the array of strings to be
   * SingleValued.
   * 
   * @param keys  an array of keys of the attributes to be constrained
   * @param single true if all should be single-valued, false if multi-valued
   */
  public void setSingleValued(String [] keys, boolean single) {
  }


  /** Returns true if a specific attribute is constrained to be SingleValued
   *
   * @param key  the key of the attribute to be checked
   *
   * @return <code>boolean</code>  true if single-valued, false if multi-valued
   */
  public boolean isSingleValued(String key) {
    return false;
  }

  /** Constrain the number of values for a specific attribute.
   * 
   * @param key the key of the attribute to be constrained
   * @param maxValues  the maximum number of values this attribute can have
   */
  public void setMaximum(String key, int maxValues) {
  }

}






