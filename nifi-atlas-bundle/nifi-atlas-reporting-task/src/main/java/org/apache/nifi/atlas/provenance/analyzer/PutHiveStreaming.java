package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.util.Tuple;

import java.net.URI;
import java.util.Set;

import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.ATTR_OUTPUT_TABLES;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.parseTableNames;

/**
 * Analyze provenance events for PutHiveStreamingProcessor.
 * <li>qualifiedName=dbName@clusterName (example: default@cl1)
 * <li>dbName (example: default)
 */
public class PutHiveStreaming extends AbstractHiveAnalyzer {

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        final URI uri = parseUri(event.getTransitUri());
        final String clusterName = context.getClusterResolver().fromHostname(uri.getHost());
        final Set<Tuple<String, String>> outputTables = parseTableNames(null, event.getAttribute(ATTR_OUTPUT_TABLES));
        if (outputTables.isEmpty()) {
            return null;
        }

        final DataSetRefs refs = new DataSetRefs(event.getComponentId());
        outputTables.forEach(tableName -> {
            final Referenceable ref = createTableRef(clusterName, tableName);
            refs.addOutput(ref);
        });
        return refs;
    }

    @Override
    public String targetComponentTypePattern() {
        return "^PutHiveStreaming$";
    }
}
