package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.schema.AbstractSchemaChangeResolver;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.parser.CustomOracleAntlrDdlParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import io.debezium.relational.ddl.DdlParser;

import java.util.List;

public class OracleSchemaChangeResolver extends AbstractSchemaChangeResolver {
    public OracleSchemaChangeResolver(SourceConfig.Factory<JdbcSourceConfig> sourceConfigFactory) {
        super(sourceConfigFactory.create(0));
    }

    @Override
    protected DdlParser createDdlParser(TablePath tablePath) {
        return new CustomOracleAntlrDdlParser(tablePath);
    }

    @Override
    protected List<AlterTableColumnEvent> getAndClearParsedEvents() {
        return ((CustomOracleAntlrDdlParser) ddlParser).getAndClearParsedEvents();
    }

    @Override
    protected String getSourceDialectName() {
        return DatabaseIdentifier.ORACLE;
    }
}
