package org.apache.nifi.atlas.provenance;

import org.apache.nifi.atlas.resolver.ClusterResolver;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;

import java.util.List;

public interface AnalysisContext {
    ClusterResolver getClusterResolver();
    List<ConnectionStatus> findConnectionTo(String componentId);
    List<ConnectionStatus> findConnectionFrom(String componentId);
    ComputeLineageResult queryLineage(long eventId);
    ProvenanceEventRecord getProvenanceEvent(long eventId);
    String lookupInputPortName (String componentId);
    String lookupOutputPortName (String componentId);
}
