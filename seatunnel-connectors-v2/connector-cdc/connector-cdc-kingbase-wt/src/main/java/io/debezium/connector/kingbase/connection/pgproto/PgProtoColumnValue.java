/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase.connection.pgproto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import com.kingbase8.geometric.KBpoint;
import com.kingbase8.jdbc.KbArray;
import com.kingbase8.util.KBmoney;
import io.debezium.connector.kingbase.KingbaseOid;
import io.debezium.connector.kingbase.KingbaseStreamingChangeEventSource;
import io.debezium.connector.kingbase.KingbaseType;
import io.debezium.connector.kingbase.KingbaseValueConverter;
import io.debezium.connector.kingbase.TypeRegistry;
import io.debezium.connector.kingbase.connection.AbstractColumnValue;
import io.debezium.connector.kingbase.connection.wal2json.DateTimeFormat;
import io.debezium.connector.kingbase.proto.PgProto;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.time.Conversions;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

/**
 * Replication message column sent by <a
 * href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</>
 *
 * @author Chris Cranford
 */
public class PgProtoColumnValue extends AbstractColumnValue<PgProto.DatumMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgProtoColumnValue.class);

    /** A number used by Kingbase to define minimum timestamp (inclusive). Defined in timestamp.h */
    private static final long TIMESTAMP_MIN = -211813488000000000L;

    /** A number used by Kingbase to define maximum timestamp (exclusive). Defined in timestamp.h */
    private static final long TIMESTAMP_MAX = 9223371331200000000L;

    // Kingbase decoderbufs V8/V9 sends several typed datum fields with wire numbers that do not
    // match the generated PostgreSQL decoderbufs class, so protobuf preserves them as unknown
    // fields. These constants map the observed Kingbase wire format back to the logical datum type.
    private static final int KINGBASE_UNKNOWN_INT64_FIELD = 5;
    private static final int KINGBASE_UNKNOWN_FLOAT_FIELD = 6;
    private static final int KINGBASE_UNKNOWN_DOUBLE_FIELD = 7;
    private static final int KINGBASE_UNKNOWN_BOOL_FIELD = 8;
    private static final int KINGBASE_UNKNOWN_POINT_FIELD = 11;

    private PgProto.DatumMessage value;

    public PgProtoColumnValue(PgProto.DatumMessage value) {
        this.value = value;
    }

    @Override
    public PgProto.DatumMessage getRawValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value.hasDatumMissing() && value.getDatumMissing();
    }

    @Override
    public String asString() {
        if (value.hasDatumString()) {
            return value.getDatumString();
        } else if (value.hasDatumBytes()) {
            return new String(asByteArray(), Charset.forName("UTF-8"));
        }
        return null;
    }

    @Override
    public Boolean asBoolean() {
        if (value.hasDatumBool()) {
            return value.getDatumBool();
        }
        Long kingbaseBool = unknownVarint(KINGBASE_UNKNOWN_BOOL_FIELD);
        if (kingbaseBool != null) {
            return kingbaseBool != 0;
        }

        final String s = asString();
        if (s != null) {
            if (s.equalsIgnoreCase("t")) {
                return Boolean.TRUE;
            } else if (s.equalsIgnoreCase("f")) {
                return Boolean.FALSE;
            }
        }
        return null;
    }

    @Override
    public Integer asInteger() {
        if (value.hasDatumInt32()) {
            return value.getDatumInt32();
        }
        if (value.hasDatumInt64()) {
            // Kingbase V9R1C10 decoderbufs can encode int2/int4 values in the int64 branch.
            return Math.toIntExact(value.getDatumInt64());
        }

        final String s = asString();
        return s != null ? Integer.valueOf(s) : null;
    }

    @Override
    public Long asLong() {
        if (value.hasDatumInt64()) {
            return value.getDatumInt64();
        }
        Long kingbaseInt64 = unknownVarint(KINGBASE_UNKNOWN_INT64_FIELD);
        if (kingbaseInt64 != null) {
            return kingbaseInt64;
        }

        final String s = asString();
        return s != null ? Long.valueOf(s) : null;
    }

    @Override
    public Float asFloat() {
        if (value.hasDatumFloat()) {
            return value.getDatumFloat();
        }
        Integer kingbaseFloatBits = unknownFixed32(KINGBASE_UNKNOWN_FLOAT_FIELD);
        if (kingbaseFloatBits != null) {
            return Float.intBitsToFloat(kingbaseFloatBits);
        }

        final String s = asString();
        return s != null ? Float.valueOf(s) : null;
    }

    @Override
    public Double asDouble() {
        if (value.hasDatumDouble()) {
            return value.getDatumDouble();
        }
        Long kingbaseDoubleBits = unknownFixed64(KINGBASE_UNKNOWN_DOUBLE_FIELD);
        if (kingbaseDoubleBits != null) {
            return Double.longBitsToDouble(kingbaseDoubleBits);
        }

        final String s = asString();
        return s != null ? Double.valueOf(s) : null;
    }

    @Override
    public Object asDecimal() {
        if (value.hasDatumDouble()) {
            return value.getDatumDouble();
        }

        final String s = asString();
        if (s != null) {
            return KingbaseValueConverter.toSpecialValue(s)
                    .orElseGet(() -> new SpecialValueDecimal(new BigDecimal(s)));
        }
        return null;
    }

    @Override
    public byte[] asByteArray() {
        return value.hasDatumBytes() ? value.getDatumBytes().toByteArray() : null;
    }

    @Override
    public LocalDate asLocalDate() {
        if (value.hasDatumInt32()) {
            return LocalDate.ofEpochDay(value.getDatumInt32());
        }
        if (value.hasDatumInt64()) {
            return LocalDate.ofEpochDay(value.getDatumInt64());
        }

        final String s = asString();
        return s != null ? DateTimeFormat.get().date(s) : null;
    }

    @Override
    public Object asTime() {
        if (value.hasDatumInt64()) {
            return Duration.of(value.getDatumInt64(), ChronoUnit.MICROS);
        }
        Long kingbaseTimeMicros = unknownVarint(KINGBASE_UNKNOWN_INT64_FIELD);
        if (kingbaseTimeMicros != null) {
            return Duration.of(kingbaseTimeMicros, ChronoUnit.MICROS);
        }

        final String s = asString();
        if (s != null) {
            return DateTimeFormat.get().time(s);
        }
        return null;
    }

    @Override
    public OffsetTime asOffsetTimeUtc() {
        if (value.hasDatumDouble()) {
            return Conversions.toInstantFromMicros((long) value.getDatumDouble())
                    .atOffset(ZoneOffset.UTC)
                    .toOffsetTime();
        }
        Long kingbaseTimeTzBits = unknownFixed64(KINGBASE_UNKNOWN_DOUBLE_FIELD);
        if (kingbaseTimeTzBits != null) {
            double micros = Double.longBitsToDouble(kingbaseTimeTzBits);
            return Conversions.toInstantFromMicros((long) micros)
                    .atOffset(ZoneOffset.UTC)
                    .toOffsetTime();
        }

        final String s = asString();
        return s != null ? DateTimeFormat.get().timeWithTimeZone(s) : null;
    }

    @Override
    public OffsetDateTime asOffsetDateTimeAtUtc() {
        if (value.hasDatumInt64()) {
            if (value.getDatumInt64() >= TIMESTAMP_MAX) {
                LOGGER.trace("Infinite(+) value '{}' arrived from database", value.getDatumInt64());
                return KingbaseValueConverter.POSITIVE_INFINITY_OFFSET_DATE_TIME;
            } else if (value.getDatumInt64() < TIMESTAMP_MIN) {
                LOGGER.trace("Infinite(-) value '{}' arrived from database", value.getDatumInt64());
                return KingbaseValueConverter.NEGATIVE_INFINITY_OFFSET_DATE_TIME;
            }
            return Conversions.toInstantFromMicros(value.getDatumInt64()).atOffset(ZoneOffset.UTC);
        }
        Long kingbaseTimestampMicros = unknownVarint(KINGBASE_UNKNOWN_INT64_FIELD);
        if (kingbaseTimestampMicros != null) {
            return Conversions.toInstantFromMicros(kingbaseTimestampMicros)
                    .atOffset(ZoneOffset.UTC);
        }

        final String s = asString();
        return s != null
                ? DateTimeFormat.get()
                        .timestampWithTimeZoneToOffsetDateTime(s)
                        .withOffsetSameInstant(ZoneOffset.UTC)
                : null;
    }

    @Override
    public Instant asInstant() {
        if (value.hasDatumInt64()) {
            if (value.getDatumInt64() >= TIMESTAMP_MAX) {
                LOGGER.trace("Infinite(+) value '{}' arrived from database", value.getDatumInt64());
                return KingbaseValueConverter.POSITIVE_INFINITY_INSTANT;
            } else if (value.getDatumInt64() < TIMESTAMP_MIN) {
                LOGGER.trace("Infinite(-) value '{}' arrived from database", value.getDatumInt64());
                return KingbaseValueConverter.NEGATIVE_INFINITY_INSTANT;
            }
            return Conversions.toInstantFromMicros(value.getDatumInt64());
        }
        Long kingbaseTimestampMicros = unknownVarint(KINGBASE_UNKNOWN_INT64_FIELD);
        if (kingbaseTimestampMicros != null) {
            return Conversions.toInstantFromMicros(kingbaseTimestampMicros);
        }

        final String s = asString();
        return s != null ? DateTimeFormat.get().timestampToInstant(asString()) : null;
    }

    @Override
    public Object asLocalTime() {
        return asTime();
    }

    @Override
    public Object asInterval() {
        if (value.hasDatumDouble()) {
            return value.getDatumDouble();
        }

        final String s = asString();
        return s != null ? super.asInterval() : null;
    }

    @Override
    public KBmoney asMoney() {
        if (value.hasDatumInt64()) {
            return new KBmoney(value.getDatumInt64() / 100.0);
        }
        return super.asMoney();
    }

    @Override
    public KBpoint asPoint() {
        if (value.hasDatumPoint()) {
            PgProto.Point datumPoint = value.getDatumPoint();
            return new KBpoint(datumPoint.getX(), datumPoint.getY());
        } else if (unknownLengthDelimited(KINGBASE_UNKNOWN_POINT_FIELD) != null) {
            try {
                PgProto.Point datumPoint =
                        PgProto.Point.parseFrom(
                                unknownLengthDelimited(KINGBASE_UNKNOWN_POINT_FIELD));
                return new KBpoint(datumPoint.getX(), datumPoint.getY());
            } catch (InvalidProtocolBufferException e) {
                LOGGER.warn(
                        "Unexpected exception trying to process Kingbase point column '{}'",
                        value.getColumnName(),
                        e);
            }
        } else if (value.hasDatumBytes()) {
            return super.asPoint();
        }
        return null;
    }

    @Override
    public boolean isArray(KingbaseType type) {
        final int oidValue = type.getOid();
        switch (oidValue) {
            case KingbaseOid.INT2_ARRAY:
            case KingbaseOid.INT4_ARRAY:
            case KingbaseOid.INT8_ARRAY:
            case KingbaseOid.TEXT_ARRAY:
            case KingbaseOid.NUMERIC_ARRAY:
            case KingbaseOid.FLOAT4_ARRAY:
            case KingbaseOid.FLOAT8_ARRAY:
            case KingbaseOid.BOOL_ARRAY:
            case KingbaseOid.DATE_ARRAY:
            case KingbaseOid.TIME_ARRAY:
            case KingbaseOid.TIMETZ_ARRAY:
            case KingbaseOid.TIMESTAMP_ARRAY:
            case KingbaseOid.TIMESTAMPTZ_ARRAY:
            case KingbaseOid.BYTEA_ARRAY:
            case KingbaseOid.VARCHAR_ARRAY:
            case KingbaseOid.OID_ARRAY:
            case KingbaseOid.BPCHAR_ARRAY:
            case KingbaseOid.MONEY_ARRAY:
            case KingbaseOid.NAME_ARRAY:
            case KingbaseOid.INTERVAL_ARRAY:
            case KingbaseOid.CHAR_ARRAY:
            case KingbaseOid.VARBIT_ARRAY:
            case KingbaseOid.UUID_ARRAY:
            case KingbaseOid.XML_ARRAY:
            case KingbaseOid.POINT_ARRAY:
            case KingbaseOid.JSONB_ARRAY:
            case KingbaseOid.JSON_ARRAY:
            case KingbaseOid.REF_CURSOR_ARRAY:
            case KingbaseOid.INET_ARRAY:
            case KingbaseOid.CIDR_ARRAY:
            case KingbaseOid.MACADDR_ARRAY:
            case KingbaseOid.MACADDR8_ARRAY:
            case KingbaseOid.TSRANGE_ARRAY:
            case KingbaseOid.TSTZRANGE_ARRAY:
            case KingbaseOid.DATERANGE_ARRAY:
            case KingbaseOid.INT4RANGE_ARRAY:
            case KingbaseOid.NUM_RANGE_ARRAY:
            case KingbaseOid.INT8RANGE_ARRAY:
                return true;
            default:
                return type.isArrayType();
        }
    }

    @Override
    public Object asArray(
            String columnName,
            KingbaseType type,
            String fullType,
            KingbaseStreamingChangeEventSource.PgConnectionSupplier connection) {
        // Currently the logical decoding plugin sends unhandled types as a byte array containing
        // the string
        // representation (in Postgres) of the array value.
        // The approach to decode this is sub-optimal but the only way to improve this is to update
        // the plugin.
        // Reasons for it being sub-optimal include:
        // 1. It requires a Postgres JDBC connection to deserialize
        // 2. The byte-array is a serialised string but we make the assumption its UTF-8 encoded
        // (which it will
        // be in most cases)
        // 3. For larger arrays and especially 64-bit integers and the like it is less efficient
        // sending string
        // representations over the wire.
        try {
            byte[] data = asByteArray();
            if (data == null) {
                return null;
            }
            String dataString = new String(data, Charset.forName("UTF-8"));
            KbArray arrayData =
                    new KbArray(connection.get(), (int) value.getColumnType(), dataString);
            Object deserializedArray = arrayData.getArray();
            return Arrays.asList((Object[]) deserializedArray);
        } catch (SQLException e) {
            LOGGER.warn(
                    "Unexpected exception trying to process KbArray column '{}'",
                    value.getColumnName(),
                    e);
        }
        return null;
    }

    @Override
    public Object asDefault(
            TypeRegistry typeRegistry,
            int columnType,
            String columnName,
            String fullType,
            boolean includeUnknownDatatypes,
            KingbaseStreamingChangeEventSource.PgConnectionSupplier connection) {
        final KingbaseType type = typeRegistry.get(columnType);
        if (type.getOid() == typeRegistry.geometryOid()
                || type.getOid() == typeRegistry.geographyOid()
                || type.getOid() == typeRegistry.citextOid()
                || type.getOid() == typeRegistry.hstoreOid()) {
            return asByteArray();
        }

        // unknown data type is sent by decoder as binary value
        if (includeUnknownDatatypes) {
            return asByteArray();
        }

        return null;
    }

    private Long unknownVarint(int fieldNumber) {
        UnknownFieldSet.Field field = value.getUnknownFields().getField(fieldNumber);
        return field.getVarintList().isEmpty() ? null : field.getVarintList().get(0);
    }

    private Integer unknownFixed32(int fieldNumber) {
        UnknownFieldSet.Field field = value.getUnknownFields().getField(fieldNumber);
        return field.getFixed32List().isEmpty() ? null : field.getFixed32List().get(0);
    }

    private Long unknownFixed64(int fieldNumber) {
        UnknownFieldSet.Field field = value.getUnknownFields().getField(fieldNumber);
        return field.getFixed64List().isEmpty() ? null : field.getFixed64List().get(0);
    }

    private ByteString unknownLengthDelimited(int fieldNumber) {
        UnknownFieldSet.Field field = value.getUnknownFields().getField(fieldNumber);
        return field.getLengthDelimitedList().isEmpty()
                ? null
                : field.getLengthDelimitedList().get(0);
    }
}
