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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;
import org.apache.seatunnel.format.text.splitor.DefaultTextLineSplitor;
import org.apache.seatunnel.format.text.splitor.TextLineSplitor;

import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class TextReadStrategy extends AbstractReadStrategy {
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private String fieldDelimiter = BaseSourceConfigOptions.FIELD_DELIMITER.defaultValue();
    private String rowDelimiter = BaseSourceConfigOptions.ROW_DELIMITER.defaultValue();
    private DateUtils.Formatter dateFormat = BaseSourceConfigOptions.DATE_FORMAT.defaultValue();
    private DateTimeUtils.Formatter datetimeFormat =
            BaseSourceConfigOptions.DATETIME_FORMAT.defaultValue();
    private TimeUtils.Formatter timeFormat = BaseSourceConfigOptions.TIME_FORMAT.defaultValue();
    private CompressFormat compressFormat = BaseSourceConfigOptions.COMPRESS_CODEC.defaultValue();
    private int[] indexes;
    private String encoding = BaseSourceConfigOptions.ENCODING.defaultValue();
    private TextLineSplitor textLineSplitor;

    /** Custom stream divider for splitting text streams by specified delimiters */
    public static class StreamLineSplitter {
        private final String delimiter;
        private final char[] delimiterChars;
        private final StringBuilder lineBuffer;
        private int delimiterIndex;
        private int skipCount;
        private final long skipHeaderNumber;
        private final LineProcessor lineProcessor;
        private final boolean useReadLine;

        public StreamLineSplitter(
                String delimiter, long skipHeaderNumber, LineProcessor lineProcessor) {
            this.delimiter = delimiter;
            this.delimiterChars = delimiter.toCharArray();
            this.lineBuffer = new StringBuilder();
            this.delimiterIndex = 0;
            this.skipCount = 0;
            this.skipHeaderNumber = skipHeaderNumber;
            this.lineProcessor = lineProcessor;

            this.useReadLine = isDefaultLineDelimiter(delimiter);
        }

        private boolean isDefaultLineDelimiter(String delimiter) {
            return "\n".equals(delimiter) || "\r\n".equals(delimiter);
        }

        public void processStream(BufferedReader reader) throws IOException {
            if (useReadLine) {
                processWithReadLine(reader);
            } else {
                processWithCharByChar(reader);
            }
        }

        private void processWithReadLine(BufferedReader reader) throws IOException {
            String line;
            int lineCount = 0;

            while ((line = reader.readLine()) != null) {
                if (lineCount >= skipHeaderNumber) {
                    if (!line.trim().isEmpty()) {
                        lineProcessor.processLine(line);
                    }
                } else {
                    lineCount++;
                }
            }
        }

        private void processWithCharByChar(BufferedReader reader) throws IOException {
            int ch;
            while ((ch = reader.read()) != -1) {
                char currentChar = (char) ch;
                processChar(currentChar);
            }

            if (lineBuffer.length() > 0) {
                if (skipCount >= skipHeaderNumber) {
                    String line = lineBuffer.toString();
                    if (!line.trim().isEmpty()) {
                        lineProcessor.processLine(line);
                    }
                }
            }
        }

        private void processChar(char currentChar) throws IOException {
            if (currentChar == delimiterChars[delimiterIndex]) {
                delimiterIndex++;
                if (delimiterIndex == delimiterChars.length) {
                    if (skipCount >= skipHeaderNumber) {
                        String line = lineBuffer.toString();
                        if (!line.trim().isEmpty()) {
                            lineProcessor.processLine(line);
                        }
                    } else {
                        skipCount++;
                    }

                    lineBuffer.setLength(0);
                    delimiterIndex = 0;
                }
            } else {
                if (delimiterIndex > 0) {
                    for (int i = 0; i < delimiterIndex; i++) {
                        lineBuffer.append(delimiterChars[i]);
                    }
                    delimiterIndex = 0;
                }
                lineBuffer.append(currentChar);
            }
        }
    }

    public interface LineProcessor {
        void processLine(String line) throws IOException;
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        InputStream inputStream;
        switch (compressFormat) {
            case LZO:
                LzopCodec lzo = new LzopCodec();
                inputStream = lzo.createInputStream(hadoopFileSystemProxy.getInputStream(path));
                break;
            case NONE:
                inputStream = hadoopFileSystemProxy.getInputStream(path);
                break;
            default:
                log.warn(
                        "Text file does not support this compress type: {}",
                        compressFormat.getCompressCodec());
                inputStream = hadoopFileSystemProxy.getInputStream(path);
                break;
        }

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(inputStream, encoding))) {

            LineProcessor lineProcessor =
                    line -> {
                        try {
                            processLineData(line, tableId, output, partitionsMap);
                        } catch (FileConnectorException e) {
                            throw new IOException(e);
                        }
                    };

            StreamLineSplitter splitter =
                    new StreamLineSplitter(rowDelimiter, skipHeaderNumber, lineProcessor);
            splitter.processStream(reader);
        }
    }

    private void processLineData(
            String line,
            String tableId,
            Collector<SeaTunnelRow> output,
            Map<String, String> partitionsMap)
            throws FileConnectorException {
        try {
            SeaTunnelRow seaTunnelRow =
                    deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8));
            if (!readColumns.isEmpty()) {
                // need column projection
                Object[] fields;
                if (isMergePartition) {
                    fields = new Object[readColumns.size() + partitionsMap.size()];
                } else {
                    fields = new Object[readColumns.size()];
                }
                for (int j = 0; j < indexes.length; j++) {
                    fields[j] = seaTunnelRow.getField(indexes[j]);
                }
                seaTunnelRow = new SeaTunnelRow(fields);
            }
            if (isMergePartition) {
                int index = seaTunnelRowType.getTotalFields();
                for (String value : partitionsMap.values()) {
                    seaTunnelRow.setField(index++, value);
                }
            }
            seaTunnelRow.setTableId(tableId);
            output.collect(seaTunnelRow);
        } catch (IOException e) {
            String errorMsg =
                    String.format(
                            "Deserialize this data [%s] failed, please check the origin data",
                            line);
            throw new FileConnectorException(
                    FileConnectorErrorCode.DATA_DESERIALIZE_FAILED, errorMsg, e);
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) {
        this.seaTunnelRowType = CatalogTableUtil.buildSimpleTextSchema();
        this.seaTunnelRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), seaTunnelRowType);
        initFormatter();
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "When reading text files, if user has not specified schema information, "
                            + "SeaTunnel will not support column projection");
        }
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        TextDeserializationSchema.Builder builder =
                TextDeserializationSchema.builder()
                        .delimiter(TextFormatConstant.PLACEHOLDER)
                        .nullFormat(
                                readonlyConfig
                                        .getOptional(BaseSourceConfigOptions.NULL_FORMAT)
                                        .orElse(null))
                        .textLineSplitor(textLineSplitor);
        ;
        if (isMergePartition) {
            deserializationSchema =
                    builder.seaTunnelRowType(this.seaTunnelRowTypeWithPartition).build();
        } else {
            deserializationSchema = builder.seaTunnelRowType(this.seaTunnelRowType).build();
        }
        return getActualSeaTunnelRowTypeInfo();
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        SeaTunnelRowType userDefinedRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), seaTunnelRowType);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        Optional<String> fieldDelimiterOptional =
                readonlyConfig.getOptional(BaseSourceConfigOptions.FIELD_DELIMITER);
        Optional<String> rowDelimiterOptional =
                readonlyConfig.getOptional(BaseSourceConfigOptions.ROW_DELIMITER);
        encoding =
                readonlyConfig
                        .getOptional(BaseSourceConfigOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
        fieldDelimiterOptional.ifPresent(s -> fieldDelimiter = s);
        rowDelimiterOptional.ifPresent(s -> rowDelimiter = s);
        initFormatter();
        TextDeserializationSchema.Builder builder =
                TextDeserializationSchema.builder()
                        .delimiter(fieldDelimiter)
                        .nullFormat(
                                readonlyConfig
                                        .getOptional(BaseSourceConfigOptions.NULL_FORMAT)
                                        .orElse(null))
                        .textLineSplitor(textLineSplitor);
        if (isMergePartition) {
            deserializationSchema =
                    builder.seaTunnelRowType(userDefinedRowTypeWithPartition).build();
        } else {
            deserializationSchema = builder.seaTunnelRowType(seaTunnelRowType).build();
        }
        // column projection
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            // get the read column index from user-defined row type
            indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = seaTunnelRowType.indexOf(readColumns.get(i));
                fields[i] = seaTunnelRowType.getFieldName(indexes[i]);
                types[i] = seaTunnelRowType.getFieldType(indexes[i]);
            }
            this.seaTunnelRowType = new SeaTunnelRowType(fields, types);
            this.seaTunnelRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.seaTunnelRowType);
        } else {
            this.seaTunnelRowType = seaTunnelRowType;
            this.seaTunnelRowTypeWithPartition = userDefinedRowTypeWithPartition;
        }
    }

    private void initFormatter() {
        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATE_FORMAT.key())) {
            dateFormat =
                    DateUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfigOptions.DATE_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATETIME_FORMAT.key())) {
            datetimeFormat =
                    DateTimeUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfigOptions.DATETIME_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.TIME_FORMAT.key())) {
            timeFormat =
                    TimeUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfigOptions.TIME_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.COMPRESS_CODEC.key())) {
            String compressCodec =
                    pluginConfig.getString(BaseSourceConfigOptions.COMPRESS_CODEC.key());
            compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
        textLineSplitor = new DefaultTextLineSplitor();
    }
}
