package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.lineage.LineageNodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.*;

/**
 * Analyze provenance events for OutputPort.
 * <li>qualifiedName=(flowFileUUID)
 */
public class OutputPort extends AbstractFileAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(OutputPort.class);

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        final List<ConnectionStatus> connections = context.findConnectionTo(event.getComponentId());
        if (connections == null || connections.isEmpty()) {
            logger.warn("Connection was not found: {}", new Object[]{event});
            return null;
        }


        //String destinationFlowFileUuid = event.getSourceSystemFlowFileIdentifier().substring("urn:nifi:".length());

        String componentName = context.lookupOutputPortName(event.getComponentId());

        final Referenceable ref = new Referenceable(TYPE_NIFI_OUTPUT_PORT);
        ref.set(ATTR_NAME, componentName);
        ref.set(ATTR_DESCRIPTION, "OutputPort -> " + event.getAttribute("filename"));
        ref.set(ATTR_QUALIFIED_NAME, event.getFlowFileUuid());

        final ProvenanceEventRecord previousEvent = findPreviousProvenanceEvent(context, event);
        if (previousEvent == null) {
            logger.warn("Previous event was not found: {}", new Object[]{event});
            return null;
        }

        final DataSetRefs refs = new DataSetRefs(previousEvent.getComponentId());
        refs.addOutput(ref);
        return refs;
    }


    private ProvenanceEventRecord findPreviousProvenanceEvent(AnalysisContext context, ProvenanceEventRecord event) {
        final ComputeLineageResult lineage = context.queryLineage(event.getEventId());
        if (lineage == null) {
            logger.warn("Lineage was not found: {}", new Object[]{event});
            return null;
        }

        // TODO: What if there is no previous node? Expired or remote_output to remote_input direct connection?
        final LineageNode previousProvenanceNode = traverseLineage(lineage, String.valueOf(event.getEventId()));
        if (previousProvenanceNode == null) {
            logger.warn("Traverse lineage could not find any preceding provenance event node: {}", new Object[]{event});
            return null;
        }

        final long previousEventId = Long.parseLong(previousProvenanceNode.getIdentifier());
        return context.getProvenanceEvent(previousEventId);
    }

    /**
     * Recursively traverse lineage graph until a preceding provenance event is found.
     */
    private LineageNode traverseLineage(ComputeLineageResult lineage, String eventId) {
        final LineageNode previousNode = lineage.getEdges().stream()
                .filter(edge -> edge.getDestination().getIdentifier().equals(String.valueOf(eventId)))
                .findFirst().map(edge -> edge.getSource()).orElse(null);
        if (previousNode == null) {
            return null;
        }
        if (previousNode.getNodeType().equals(LineageNodeType.PROVENANCE_EVENT_NODE)) {
            return previousNode;
        }
        return traverseLineage(lineage, previousNode.getIdentifier());
    }

    @Override
    public String targetComponentTypePattern() {
        return "^Output Port$";
    }
}
