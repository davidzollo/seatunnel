package io.debezium.connector.kingbase.connection;

import com.kingbase8.util.KBmoney;

import java.lang.reflect.Field;

/** Compatibility accessor for KBmoney across Kingbase JDBC driver versions. */
final class KingbaseMoneyUtil {

    private static final Field VALUE_FIELD = resolveField("value");
    private static final Field VAL_FIELD = resolveField("val");

    private KingbaseMoneyUtil() {}

    static Object numericValue(KBmoney money) {
        if (money == null) {
            return null;
        }

        Object value = readField(VALUE_FIELD, money);
        if (value != null) {
            return value;
        }

        value = readField(VAL_FIELD, money);
        if (value != null) {
            return value;
        }

        return money;
    }

    private static Field resolveField(String name) {
        try {
            return KBmoney.class.getField(name);
        } catch (NoSuchFieldException e) {
            return null;
        }
    }

    private static Object readField(Field field, KBmoney money) {
        if (field == null) {
            return null;
        }

        try {
            return field.get(money);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot access KBmoney field: " + field.getName(), e);
        }
    }
}
