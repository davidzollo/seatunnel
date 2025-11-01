/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.config;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test SqlServerSourceConfigFactory table name escaping functionality */
public class SqlServerSourceConfigFactoryTest {

    @Test
    void testEscapeTableNameForRegex() throws Exception {
        Method method =
                SqlServerSourceConfigFactory.class.getDeclaredMethod(
                        "escapeTableNameForRegex", String.class);
        method.setAccessible(true);

        // Test normal table name
        String result = (String) method.invoke(null, "users");
        assertEquals("\\Qusers\\E", result);

        // Test table name with spaces
        result = (String) method.invoke(null, "Employee Records");
        assertEquals("\\QEmployee Records\\E", result);

        // Test table name with dollar sign
        result = (String) method.invoke(null, "Maxincome Resources Sdn Bhd$Planogram Information");
        assertEquals("\\QMaxincome Resources Sdn Bhd$Planogram Information\\E", result);

        // Test table name with multiple special characters
        result = (String) method.invoke(null, "Table$With.Special[Chars]");
        assertEquals("\\QTable$With.Special[Chars]\\E", result);

        // Test null input
        result = (String) method.invoke(null, (String) null);
        assertNull(result);

        // Test empty string
        result = (String) method.invoke(null, "");
        assertEquals("\\Q\\E", result);
    }
}
