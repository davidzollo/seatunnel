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
import org.apache.seatunnel.common.utils.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;

public class SerializerAdapter<T extends Serializable> extends DefaultSerializer<T> {
    @Override
    public T deserialize(byte[] serialized) throws IOException {
        if (serialized == null) {
            return null;
        }
        try (ByteArrayInputStream s = new ByteArrayInputStream(serialized);
                ObjectInputStream in = new ObjectInputStreamAdapter(s)) {
            @SuppressWarnings("unchecked")
            final T obj = (T) in.readObject();
            return obj;
        } catch (final ClassNotFoundException | IOException ex) {
            throw new SerializationException(ex);
        }
    }

    private static class ObjectInputStreamAdapter extends ObjectInputStream {

        public ObjectInputStreamAdapter(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if (cl == null) {
                return super.resolveClass(desc);
            }
            return Class.forName(desc.getName(), false, cl);
        }

        @Override
        protected ObjectStreamClass readClassDescriptor()
                throws IOException, ClassNotFoundException {
            ObjectStreamClass incoming = super.readClassDescriptor();
            if (incoming.getName().equals(SinkIdentifier.class.getName())) {
                // compatible with old version class
                ObjectStreamClass local = ObjectStreamClass.lookup(SinkIdentifier.class);
                if (local != null) {
                    return local;
                }
            }
            return incoming;
        }
    }
}
