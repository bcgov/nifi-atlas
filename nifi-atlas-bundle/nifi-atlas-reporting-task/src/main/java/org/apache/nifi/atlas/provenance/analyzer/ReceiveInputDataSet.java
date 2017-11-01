package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.nifi.atlas.NiFiTypes.*;

/**
 * Analyze a RECEIVE event and create 'nifi_data' when there is no specific Analyzer implementation found.
 * <li>qualifiedName=FlowFileUuid
 * <li>name=NiFiComponentType (example: GenerateFlowFile)
 */
public class ReceiveInputDataSet extends AbstractNiFiProvenanceEventAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(ReceiveInputDataSet.class);

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        // Check if this component is a processor that generates data.
        final String componentId = event.getComponentId();
        final List<ConnectionStatus> incomingConnections = context.findConnectionTo(componentId);
        if (incomingConnections != null && !incomingConnections.isEmpty()) {
            logger.info("INCOMING CONNECTIONS, SKIPPING");
            return null;
        }

        final Referenceable ref = new Referenceable(TYPE_NIFI_DATA);
        ref.set(ATTR_NAME, event.getComponentType() + " : " + event.getComponentId());
        ref.set(ATTR_DESCRIPTION, event.getTransitUri());
        ref.set(ATTR_QUALIFIED_NAME, event.getFlowFileUuid());

        logger.info("ReceiveInputDataSet GENERIC " + event.getEventType() + " : " + event.getComponentType());

        final DataSetRefs refs = new DataSetRefs(componentId);
        refs.addInput(ref);

        return refs;
    }

    @Override
    public ProvenanceEventType targetProvenanceEventType() {
        return ProvenanceEventType.RECEIVE;
    }
}
