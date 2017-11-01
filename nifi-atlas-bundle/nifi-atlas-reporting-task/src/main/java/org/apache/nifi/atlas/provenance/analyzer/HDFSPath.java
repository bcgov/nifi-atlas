package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.net.URI;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_CLUSTER_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_PATH;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze a transit URI as a HDFS path.
 * <li>qualifiedName=/path/fileName@clusterName (example: /app/warehouse/hive/db/default@cl1)
 * <li>name=/path/fileName (example: /app/warehouse/hive/db/default)
 */
public class HDFSPath extends AbstractNiFiProvenanceEventAnalyzer {

    private static final String TYPE = "hdfs_path";

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        final Referenceable ref = new Referenceable(TYPE);
        final URI uri = parseUri(event.getTransitUri());
        final String clusterName = context.getClusterResolver().fromHostname(uri.getHost());
        final String path = uri.getPath();
        ref.set(ATTR_NAME, path);
        ref.set(ATTR_PATH, path);
        ref.set(ATTR_CLUSTER_NAME, clusterName);
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, path));

        return singleDataSetRef(event.getComponentId(), event.getEventType(), ref);
    }

    @Override
    public String targetTransitUriPattern() {
        return "^hdfs://.+$";
    }
}
