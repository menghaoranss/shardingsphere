/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.impl.projection.impl.column;

import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.AbstractExpectedIdentifierSQLSegment;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.impl.bound.ExpectedColumnBoundInfo;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.impl.generic.ExpectedParentheses;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.impl.projection.ExpectedProjection;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.impl.table.ExpectedOwner;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

/**
 * Expected column projection.
 */
@Getter
@Setter
public final class ExpectedColumnProjection extends AbstractExpectedIdentifierSQLSegment implements ExpectedProjection {
    
    @XmlAttribute
    private String alias;
    
    @XmlElement
    private ExpectedOwner owner;
    
    @XmlElement(name = "left-parentheses")
    private ExpectedParentheses leftParentheses;
    
    @XmlElement(name = "right-parentheses")
    private ExpectedParentheses rightParentheses;
    
    @XmlElement(name = "column-bound")
    private ExpectedColumnBoundInfo columnBound;
}
