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

package io.debezium.connector.kingbase.connection.pgproto;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.protobuf.CodedOutputStream;
import com.kingbase8.geometric.KBpoint;
import io.debezium.connector.kingbase.KingbaseType;
import io.debezium.connector.kingbase.TypeRegistry;
import io.debezium.connector.kingbase.connection.ReplicationMessage;
import io.debezium.connector.kingbase.connection.ReplicationMessageColumnValueResolver;
import io.debezium.connector.kingbase.proto.PgProto;
import io.debezium.time.Conversions;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Verifies Kingbase V9R1C10 decoderbufs wire messages, whose schema-aware field layout differs from
 * the upstream PostgreSQL decoderbufs proto used by Debezium.
 */
public class PgProtoMessageDecoderTest {

    private static final String KINGBASE_INSERT_PAYLOAD =
            "088b0610c08c8cabd7fc93031a067075626c6963221273656174756e6e656c5f6364635f"
                    + "64756d70280032130a026964101718ffffffffffffffffff01200132120a046e616d65"
                    + "10930818444a056669727374420b0a07696e7465676572100042190a15636861726163"
                    + "7465722076617279696e6728363429100150e8e69238";

    @Test
    public void testDecodeKingbaseSchemaAwareInsertMessage() throws Exception {
        PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();
        AtomicReference<ReplicationMessage> actualMessage = new AtomicReference<>();

        decoder.processNotEmptyMessage(
                ByteBuffer.wrap(hexToBytes(KINGBASE_INSERT_PAYLOAD)), actualMessage::set, null);

        Assertions.assertNotNull(actualMessage.get());
        Assertions.assertEquals(
                ReplicationMessage.Operation.INSERT, actualMessage.get().getOperation());
        Assertions.assertEquals("public.seatunnel_cdc_dump", actualMessage.get().getTable());
    }

    @Test
    public void testDecodeKingbaseInt4EncodedAsInt64() {
        PgProto.DatumMessage datum =
                PgProto.DatumMessage.newBuilder()
                        .setColumnName("id")
                        .setColumnType(23)
                        .setDatumInt64(4L)
                        .build();

        Assertions.assertEquals(4, new PgProtoColumnValue(datum).asInteger());
    }

    @Test
    public void testDecodeDatumValueWhenTrailingMissingFalseIsPresent() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.writeString(1, "c");
        output.writeInt64(2, 20L);
        output.writeInt64(4, 20000000000L);
        output.writeBool(11, false);
        output.flush();

        PgProto.DatumMessage datum = PgProto.DatumMessage.parseFrom(bytes.toByteArray());

        Assertions.assertTrue(datum.hasDatumInt64());
        Assertions.assertEquals(20000000000L, datum.getDatumInt64());
        Assertions.assertFalse(new PgProtoColumnValue(datum).isNull());
    }

    @Test
    public void testDecodeBooleanFalseWhenTrailingMissingFalseIsPresent() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.writeString(1, "c");
        output.writeInt64(2, 16L);
        output.writeBool(7, false);
        output.writeBool(11, false);
        output.flush();

        PgProto.DatumMessage datum = PgProto.DatumMessage.parseFrom(bytes.toByteArray());

        Assertions.assertTrue(datum.hasDatumBool());
        Assertions.assertFalse(datum.getDatumBool());
        Assertions.assertFalse(new PgProtoColumnValue(datum).isNull());
    }

    @Test
    public void testDecodePointWhenTrailingMissingFalseIsPresent() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        PgProto.Point point = PgProto.Point.newBuilder().setX(3.3D).setY(4.4D).build();
        output.writeString(1, "c");
        output.writeInt64(2, 600L);
        output.writeMessage(10, point);
        output.writeBool(11, false);
        output.flush();

        PgProto.DatumMessage datum = PgProto.DatumMessage.parseFrom(bytes.toByteArray());

        Assertions.assertTrue(datum.hasDatumPoint());
        Assertions.assertEquals(point, datum.getDatumPoint());
        Assertions.assertFalse(new PgProtoColumnValue(datum).isNull());
    }

    @Test
    public void testDatumMissingRequiresTrueValue() {
        PgProto.DatumMessage missingFalse =
                PgProto.DatumMessage.newBuilder().setDatumMissing(false).build();
        PgProto.DatumMessage missingTrue =
                PgProto.DatumMessage.newBuilder().setDatumMissing(true).build();

        Assertions.assertFalse(new PgProtoColumnValue(missingFalse).isNull());
        Assertions.assertTrue(new PgProtoColumnValue(missingTrue).isNull());
    }

    @Test
    public void testDecodeKingbaseUnknownScalarWireFields() throws Exception {
        PgProto.DatumMessage boolFalse =
                parseDatum(
                        16L,
                        output -> {
                            output.writeInt32(3, -1);
                            output.writeBool(8, false);
                        });
        PgProto.DatumMessage int8 =
                parseDatum(
                        20L,
                        output -> {
                            output.writeInt32(3, -1);
                            output.writeInt64(5, 20000000000L);
                        });
        PgProto.DatumMessage float4 =
                parseDatum(
                        700L,
                        output -> {
                            output.writeInt32(3, -1);
                            output.writeFloat(6, 2.5F);
                        });
        PgProto.DatumMessage float8 =
                parseDatum(
                        701L,
                        output -> {
                            output.writeInt32(3, -1);
                            output.writeDouble(7, 2.345678D);
                        });

        Assertions.assertFalse(new PgProtoColumnValue(boolFalse).asBoolean());
        Assertions.assertEquals(20000000000L, new PgProtoColumnValue(int8).asLong());
        Assertions.assertEquals(2.5F, new PgProtoColumnValue(float4).asFloat(), 0.000001F);
        Assertions.assertEquals(2.345678D, new PgProtoColumnValue(float8).asDouble(), 0.000001D);
    }

    @Test
    public void testDecodeKingbaseUnknownTemporalAndPointWireFields() throws Exception {
        PgProto.DatumMessage date =
                PgProto.DatumMessage.newBuilder()
                        .setColumnName("c")
                        .setColumnType(1082L)
                        .setDatumInt64(20564L)
                        .build();
        PgProto.DatumMessage time =
                parseDatum(
                        1083L,
                        output -> {
                            output.writeInt32(3, -1);
                            output.writeInt64(5, 48896123456L);
                        });
        PgProto.DatumMessage timestamp =
                parseDatum(
                        1114L,
                        output -> {
                            output.writeInt32(3, -1);
                            output.writeInt64(5, 1776774896123456L);
                        });
        PgProto.Point point = PgProto.Point.newBuilder().setX(3.3D).setY(4.4D).build();
        PgProto.DatumMessage pointDatum =
                parseDatum(
                        600L,
                        output -> {
                            output.writeInt32(3, -1);
                            output.writeMessage(11, point);
                        });

        KBpoint actualPoint = new PgProtoColumnValue(pointDatum).asPoint();

        Assertions.assertEquals(
                LocalDate.of(2026, 4, 21), new PgProtoColumnValue(date).asLocalDate());
        Assertions.assertEquals(
                Duration.of(48896123456L, ChronoUnit.MICROS),
                new PgProtoColumnValue(time).asTime());
        Assertions.assertEquals(
                Conversions.toInstantFromMicros(1776774896123456L),
                new PgProtoColumnValue(timestamp).asInstant());
        Assertions.assertEquals(3.3D, actualPoint.x, 0.000001D);
        Assertions.assertEquals(4.4D, actualPoint.y, 0.000001D);
    }

    @Test
    public void testResolveKingbaseRootDateType() {
        KingbaseType dateType =
                new KingbaseType.Builder(
                                null, "date", 1082, Types.DATE, TypeRegistry.NO_TYPE_MODIFIER, null)
                        .build();
        PgProto.DatumMessage date =
                PgProto.DatumMessage.newBuilder()
                        .setColumnName("c")
                        .setColumnType(1082L)
                        .setDatumInt64(20564L)
                        .build();

        Object resolved =
                ReplicationMessageColumnValueResolver.resolveValue(
                        "c", dateType, "date", new PgProtoColumnValue(date), null, false, null);

        Assertions.assertEquals(LocalDate.of(2026, 4, 21), resolved);
    }

    private static byte[] hexToBytes(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    private static PgProto.DatumMessage parseDatum(long columnType, DatumWriter writer)
            throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.writeString(1, "c");
        output.writeInt64(2, columnType);
        writer.write(output);
        output.flush();
        return PgProto.DatumMessage.parseFrom(bytes.toByteArray());
    }

    private interface DatumWriter {
        void write(CodedOutputStream output) throws Exception;
    }
}
