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

package org.apache.seatunnel.connectors.seatunnel.common.multitablesink;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.common.utils.SerializationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InvalidClassException;
import java.util.Base64;

public class SerializerAdapterTest {
    private static final String OLD_STATE =
            "rO0ABXNyAE9vcmcuYXBhY2hlLnNlYXR1bm5lbC5jb25uZWN0b3JzLnNlYXR1bm5lbC5jb21tb24ubXVsdGl0YWJsZXNpbmsuTXVsdGlUYWJsZVN0YXRlxiqtYG4Qa4sCAAFMAAZzdGF0ZXN0AA9MamF2YS91dGlsL01hcDt4cHNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAABc3IATm9yZy5hcGFjaGUuc2VhdHVubmVsLmNvbm5lY3RvcnMuc2VhdHVubmVsLmNvbW1vbi5tdWx0aXRhYmxlc2luay5TaW5rSWRlbnRpZmllclDUGMj8cNLhAgACSQAFaW5kZXhMAA90YWJsZUlkZW50aWZpZXJ0ABJMamF2YS9sYW5nL1N0cmluZzt4cAAAAAF0AAFhc3IAGmphdmEudXRpbC5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAFhdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAFFtMamF2YS5sYW5nLkludGVnZXI7/petoAGD4hsCAAB4cAAAAAJzcgARamF2YS5sYW5nLkludGVnZXIS4qCk94GHOAIAAUkABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwAAAAAXNxAH4ADgAAAAJ4";
    private static final String NEW_STATE =
            "rO0ABXNyAE9vcmcuYXBhY2hlLnNlYXR1bm5lbC5jb25uZWN0b3JzLnNlYXR1bm5lbC5jb21tb24ubXVsdGl0YWJsZXNpbmsuTXVsdGlUYWJsZVN0YXRlxiqtYG4Qa4sCAAFMAAZzdGF0ZXN0AA9MamF2YS91dGlsL01hcDt4cHNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAABc3IATm9yZy5hcGFjaGUuc2VhdHVubmVsLmNvbm5lY3RvcnMuc2VhdHVubmVsLmNvbW1vbi5tdWx0aXRhYmxlc2luay5TaW5rSWRlbnRpZmllcm+9ME5nzV7nAgACSQAFaW5kZXhMAA90YWJsZUlkZW50aWZpZXJ0ABJMamF2YS9sYW5nL1N0cmluZzt4cAAAAAF0AAFhc3IAGmphdmEudXRpbC5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAFhdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAFFtMamF2YS5sYW5nLkludGVnZXI7/petoAGD4hsCAAB4cAAAAAJzcgARamF2YS5sYW5nLkludGVnZXIS4qCk94GHOAIAAUkABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwAAAAAXNxAH4ADgAAAAJ4";

    @Test
    void testDeserializeError() {
        Serializer<MultiTableState> serializer = new DefaultSerializer<>();
        try {
            deserialize(serializer, OLD_STATE);
            deserialize(serializer, NEW_STATE);
            Assertions.fail("Should throw exception");
        } catch (SerializationException | IOException e) {
            Assertions.assertInstanceOf(InvalidClassException.class, e.getCause());
            Assertions.assertEquals(
                    "org.apache.seatunnel.connectors.seatunnel.common.multitablesink.SinkIdentifier; local class incompatible: stream classdesc serialVersionUID = 5824307469604672225, local class serialVersionUID = 8051644822115409639",
                    e.getCause().getMessage());
        }
    }

    @Test
    void testDeserializeWithCompatibility() {
        Serializer<MultiTableState> serializer = new SerializerAdapter<>();
        try {
            MultiTableState oldState = deserialize(serializer, OLD_STATE);
            MultiTableState newState = deserialize(serializer, NEW_STATE);
            Assertions.assertEquals(oldState.getStates(), newState.getStates());
        } catch (Throwable e) {
            Assertions.fail(e);
        }
    }

    static MultiTableState deserialize(Serializer<MultiTableState> serializer, String base64)
            throws IOException {
        byte[] bytes = Base64.getDecoder().decode(base64);
        return serializer.deserialize(bytes);
    }
}
