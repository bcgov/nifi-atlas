package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DatabaseAnalyzerUtil {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseAnalyzerUtil.class);

    public static String ATTR_INPUT_TABLES = "query.input.tables";
    public static String ATTR_OUTPUT_TABLES = "query.output.tables";

    public static  Set<Tuple<String, String>> parseTableNames(String connectedDatabaseName, String tableNamesStr) {
        if (tableNamesStr == null || tableNamesStr.isEmpty()) {
            return Collections.emptySet();
        }
        return Arrays.stream(tableNamesStr.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(t -> DatabaseAnalyzerUtil.parseTableName(connectedDatabaseName, t))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    private static  Tuple<String, String> parseTableName(String connectedDatabaseName, String tableNameStr) {
        final String[] tableNameSplit = tableNameStr.split("\\.");
        if (tableNameSplit.length != 1 && tableNameSplit.length != 2) {
            logger.warn("Unexpected table name format: {}", tableNameStr);
            return null;
        }
        final String databaseName = tableNameSplit.length == 2 ? tableNameSplit[0] : connectedDatabaseName;
        final String tableName = tableNameSplit.length == 2 ? tableNameSplit[1] : tableNameSplit[0];
        return new Tuple<>(databaseName, tableName);
    }

    public static String toTableNameStr(Tuple<String, String> tableName) {
        return toTableNameStr(tableName.getKey(), tableName.getValue());
    }

    public static String toTableNameStr(String databaseName, String tableName) {
        return String.format("%s.%s", databaseName, tableName);
    }

}
