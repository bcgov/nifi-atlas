package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.net.URI;

import static org.apache.nifi.atlas.NiFiTypes.*;

/**
 * Analyze provenance events for (Get|Put)File.
 * <li>qualifiedName=(absolutePath)(filename)@clusterName (example: default@cl1)
 * <li>dbName (example: default)
 */
public class GetPutFile extends AbstractFileAnalyzer {

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        final URI uri = parseUri(event.getTransitUri());
        final String clusterName = context.getClusterResolver().fromHostname(uri.getHost());

        final DataSetRefs refs = new DataSetRefs(event.getComponentId());
        final Referenceable ref = createFileRef(clusterName, uri);

        ref.set(ATTR_NAME, event.getAttribute("filename"));
        ref.set(ATTR_PATH, event.getAttribute("path"));

        ref.set(ATTR_IS_FILE, true);
        ref.set(ATTR_FILE_SIZE, event.getFileSize());
        ref.set(ATTR_FILE_GROUP, event.getAttribute("file.group"));
        ref.set(ATTR_FILE_OWNER, event.getAttribute("file.owner"));

        if (event.getComponentType().equals("GetFile")) {
            refs.addInput(ref);
        } else {
            refs.addOutput(ref);
        }

        return refs;
    }

    @Override
    public String targetComponentTypePattern() {
        return "^(Get|Put)File$";
    }
}
