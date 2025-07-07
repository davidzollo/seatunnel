package org.apache.seatunnel.connectors.cdc.dameng.utils;

import org.apache.commons.codec.DecoderException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.dameng.DamengValueConverters;

public class DamengValueConvertersHexToFixedLengthBytesTest {

    @Test
    void testHexToFixedLengthBytes_evenLength() throws DecoderException {
        String hex = "000A1B2C";
        byte[] result = DamengValueConverters.hexToFixedLengthBytes(hex, 4);
        Assertions.assertArrayEquals(new byte[] {0, 0x0A, 0x1B, 0x2C}, result);
    }

    @Test
    void testHexToFixedLengthBytes_oddLength() throws DecoderException {
        String hex = "A1B2C";
        byte[] result = DamengValueConverters.hexToFixedLengthBytes(hex, 3);
        Assertions.assertArrayEquals(new byte[] {0x0A, 0x1B, 0x2C}, result);
    }

    @Test
    void testHexToFixedLengthBytes_trimSpaces() throws DecoderException {
        String hex = "  0A1B2C  ";
        byte[] result = DamengValueConverters.hexToFixedLengthBytes(hex, 3);
        Assertions.assertArrayEquals(new byte[] {0x0A, 0x1B, 0x2C}, result);
    }

    @Test
    void testHexToFixedLengthBytes_shorterThanLength() throws DecoderException {
        String hex = "0A1B";
        byte[] result = DamengValueConverters.hexToFixedLengthBytes(hex, 4);
        Assertions.assertEquals(4, result.length);
        Assertions.assertEquals(0x0A, result[0]);
        Assertions.assertEquals(0x1B, result[1]);
        Assertions.assertEquals(0, result[2]);
        Assertions.assertEquals(0, result[3]);
    }

    @Test
    void testHexToFixedLengthBytes_invalidHex() {
        Assertions.assertThrows(
                DecoderException.class, () -> DamengValueConverters.hexToFixedLengthBytes("ZZ", 1));
    }
}
