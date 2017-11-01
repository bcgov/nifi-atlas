package org.apache.nifi.atlas.provenance;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.NiFIAtlasHook;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.NiFiFlowPath;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.*;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;

public class ByPathLineageStrategy implements LineageEventProcessor {
    ComponentLog logger;
    NiFIAtlasHook nifiAtlasHook;

    public ByPathLineageStrategy (ComponentLog logger, NiFIAtlasHook atlasHook) {
        this.logger = logger;
        this.nifiAtlasHook = atlasHook;
    }

    private ComponentLog getLogger() {
        return logger;
    }

    public void processEvent (ProvenanceEventRecord event, NiFiFlow nifiFlow, AnalysisContext analysisContext) {
        try {
            final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(event.getComponentType(), event.getTransitUri(), event.getEventType());
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Analyzer {} is found for event: {}", new Object[]{analyzer, event});
            }
            if (analyzer == null) {
                return;
            }
            final DataSetRefs refs = analyzer.analyze(analysisContext, event);
            if (refs == null || (refs.isEmpty())) {
                return;
            }

            // TODO: need special logic for remote ports as it may be connected to multiple flow paths.
            final Set<NiFiFlowPath> flowPaths = refs.getComponentIds().stream()
                    .map(componentId -> {
                        final NiFiFlowPath flowPath = nifiFlow.findPath(componentId);
                        if (flowPath == null) {
                            getLogger().warn("FlowPath for {} was not found.", new Object[]{event.getComponentId()});
                        }
                        return flowPath;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

            // create reference to NiFi flow path.
            for (NiFiFlowPath flowPath : flowPaths) {
                // TODO: make the reference to NiFiFlow optional?
                final Referenceable flowRef = new Referenceable(TYPE_NIFI_FLOW);
                flowRef.set(ATTR_NAME, nifiFlow.getFlowName());
                flowRef.set(ATTR_QUALIFIED_NAME, nifiFlow.getId().getUniqueAttributes().get(ATTR_QUALIFIED_NAME));
                flowRef.set(ATTR_URL, nifiFlow.getUrl());

                final Referenceable flowPathRef = new Referenceable(TYPE_NIFI_FLOW_PATH);
                flowPathRef.set(ATTR_NAME, flowPath.getName());
                flowPathRef.set(ATTR_QUALIFIED_NAME, flowPath.getId());
                flowPathRef.set(ATTR_NIFI_FLOW, flowRef);
                flowPathRef.set(ATTR_URL, nifiFlow.getUrl());

                nifiAtlasHook.addDataSetRefs(refs, flowPathRef, false);
            }

        } catch (Exception e) {
            // If something went wrong, log it and continue with other records.
            getLogger().error("Skipping failed analyzing event {} due to {}.", new Object[]{event, e}, e);
        }
    }
}
