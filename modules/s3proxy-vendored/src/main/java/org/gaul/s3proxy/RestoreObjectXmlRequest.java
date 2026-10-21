/*
 * Copyright 2014-2026 Andrew Gaul <andrew@gaul.org>
 * Copyright 2026 Deutsches Elektronen-Synchrotron
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gaul.s3proxy;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import tools.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * dCache patch: the body of a RestoreObject request.  Only Days is read; the
 * retrieval tier and the select parameters do not apply.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
record RestoreObjectXmlRequest(
        @JacksonXmlProperty(localName = "Days") Integer days) {
}
