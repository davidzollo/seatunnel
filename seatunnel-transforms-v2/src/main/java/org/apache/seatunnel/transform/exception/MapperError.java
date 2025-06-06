package org.apache.seatunnel.transform.exception;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.transform.exception.MapperErrorCode.WRONG_SQL_FUNCTION;

public class MapperError {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static TransformException fieldWithWrongSqlFunction(
            Map<String, List<Map<String, String>>> wrongField) {
        Map<String, String> params = new HashMap<>();
        try {
            params.put("wrongField", OBJECT_MAPPER.writeValueAsString(wrongField));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new TransformException(WRONG_SQL_FUNCTION, params);
    }
}
