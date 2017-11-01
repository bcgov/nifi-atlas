package org.apache.nifi.atlas.provenance;

import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public interface LineageEventProcessor {
    void processEvent (ProvenanceEventRecord event, NiFiFlow nifiFlow, AnalysisContext analysisContext);
}
