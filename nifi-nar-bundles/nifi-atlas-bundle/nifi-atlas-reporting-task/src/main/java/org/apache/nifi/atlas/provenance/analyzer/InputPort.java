package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.*;

/**
 * Analyze provenance events for InputPort.
 * <li>qualifiedName=(flowFileUUID)
 */
public class InputPort extends AbstractFileAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(InputPort.class);

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        final List<ConnectionStatus> connections = context.findConnectionFrom(event.getComponentId());
        if (connections == null || connections.isEmpty()) {
            logger.warn("Connection was not found: {}", new Object[]{event});
            return null;
        }


        String sourceFlowFileUuid = event.getSourceSystemFlowFileIdentifier().substring("urn:nifi:".length());

        String componentName = context.lookupInputPortName(event.getComponentId());

        final Referenceable ref = new Referenceable(TYPE_NIFI_INPUT_PORT);
        ref.set(ATTR_NAME, componentName);
        ref.set(ATTR_QUALIFIED_NAME, sourceFlowFileUuid);

        // reference all the connections from this port and add the Input Port
        // as an input to each of those connections.
        final Set<String> connectedComponentIds = connections.stream()
                .map(c -> c.getDestinationId()).collect(Collectors.toSet());
        final DataSetRefs refs = new DataSetRefs(connectedComponentIds);
        refs.addInput(ref);

        return refs;
    }

    @Override
    public String targetComponentTypePattern() {
        return "^Input Port$";
    }
}
