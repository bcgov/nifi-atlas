package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.lang.StringUtils;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
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

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;

/**
 * Analyze a transit URI as a NiFi Site-to-Site remote input/output port.
 * <li>qualifiedName=remotePortGUID (example: 35dbc0ab-015e-1000-144c-a8d71255027d)
 * <li>name=portName (example: input)
 */
public class NiFiRemotePort extends AbstractNiFiProvenanceEventAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRemotePort.class);

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        final boolean isRemoteInputPort = event.getComponentType().equals("Remote Input Port");
        final String type = isRemoteInputPort ? TYPE_NIFI_INPUT_PORT : TYPE_NIFI_OUTPUT_PORT;
        final String remotePortId = event.getComponentId();

        // TODO: What if the connected component is not a processor such as Funnel?
        final List<ConnectionStatus> connections = isRemoteInputPort
                ? context.findConnectionTo(remotePortId)
                : context.findConnectionFrom(remotePortId);
        if (connections == null || connections.isEmpty()) {
            logger.warn("Connection was not found: {}", new Object[]{event});
            return null;
        }


        // For RemoteInputPort, need to find the previous component connected to this port,
        // which passed this particular FlowFile.
        // That is only possible by calling lineage API.
        final DataSetRefs refs;
        if (isRemoteInputPort) {
            // There will eventually be an output port SEND that will set the NAME appropriately
            // The name of remote port can be retrieved from any connection, use the first one.
            final ConnectionStatus connection = connections.get(0);
            final Referenceable ref = new Referenceable(type);
            ref.set(ATTR_NAME, isRemoteInputPort ? connection.getDestinationName() : connection.getSourceName());
            ref.set(ATTR_QUALIFIED_NAME, event.getFlowFileUuid()); // event.getComponentId();

            final ProvenanceEventRecord previousEvent = findPreviousProvenanceEvent(context, event);
            if (previousEvent == null) {
                logger.warn("Previous event was not found: {}", new Object[]{event});
                return null;
            }

            refs = new DataSetRefs(previousEvent.getComponentId());
            refs.addOutput(ref);
        } else {
            String sourceFlowFileUuid = event.getSourceSystemFlowFileIdentifier().substring("urn:nifi:".length());

            String componentName = context.lookupOutputPortName(event.getComponentId());

            final Referenceable ref = new Referenceable(TYPE_NIFI_OUTPUT_PORT);
            ref.set(ATTR_NAME, componentName);
            ref.set(ATTR_QUALIFIED_NAME, sourceFlowFileUuid);

            // Ror RemoteOutputPort, it's possible that multiple processors are connected.
            // In that case, the received FlowFile is cloned and passed to each connection.
            // So we need to create multiple DataSetRefs.
            final Set<String> connectedComponentIds = connections.stream()
                    .map(c -> c.getDestinationId()).collect(Collectors.toSet());
            refs = new DataSetRefs(connectedComponentIds);
            refs.addInput(ref);
        }

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
        return "^Remote (In|Out)put Port$";
    }
}
