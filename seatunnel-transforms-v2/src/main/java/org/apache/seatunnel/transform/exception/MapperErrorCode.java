package org.apache.seatunnel.transform.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum MapperErrorCode implements SeaTunnelErrorCode {
    WRONG_SQL_FUNCTION("MAPPER-01", "The Mapper target field type had wrong: '<wrongField>'"),
    ;

    private final String code;
    private final String description;

    MapperErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
