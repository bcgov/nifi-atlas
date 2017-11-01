package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.util.List;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze a CREATE event and create 'nifi_data' when there is no specific Analyzer implementation found.
 * <li>qualifiedName=NiFiComponentId (example: processor GUID)
 * <li>name=NiFiComponentType (example: GenerateFlowFile)
 */
public class CreateObscureInputDataSet extends AbstractNiFiProvenanceEventAnalyzer {

    private static final String TYPE = "nifi_data";

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        // Check if this component is a processor that generates data.
        final String componentId = event.getComponentId();
        final List<ConnectionStatus> incomingConnections = context.findConnectionTo(componentId);
        if (incomingConnections != null && !incomingConnections.isEmpty()) {
            return null;
        }

        final Referenceable ref = new Referenceable(TYPE);
        ref.set(ATTR_NAME, event.getComponentType());
        ref.set(ATTR_QUALIFIED_NAME, componentId);

        // CREATE would mean creating output DataSet in general,
        // but this Analyzer creates input DataSet for NiFi.
        final DataSetRefs refs = new DataSetRefs(componentId);
        refs.addInput(ref);

        return refs;
    }

    @Override
    public ProvenanceEventType targetProvenanceEventType() {
        return ProvenanceEventType.CREATE;
    }
}
