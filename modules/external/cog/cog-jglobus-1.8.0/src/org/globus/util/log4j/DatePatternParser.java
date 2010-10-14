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
package org.globus.util.log4j;

import org.apache.log4j.helpers.PatternParser;

public class DatePatternParser extends PatternParser {
    
    public DatePatternParser(String pattern) {
        super(pattern);
    }
    
    protected void finalizeConverter(char c) {       
      if (c == 'd') {  
          int previousIndex = this.i;
          String dOpt = extractOption();
          if (dOpt != null && dOpt.startsWith("ISO8601")) {
              String params = dOpt.substring("ISO8601".length());
              addConverter(new DatePatternConverter(this.formattingInfo, params));
              this.currentLiteral.setLength(0);
          } else {
              this.i = previousIndex;
              super.finalizeConverter(c);
         }
      } else {
          super.finalizeConverter(c);
      }
    }
}
