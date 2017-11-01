package org.apache.nifi.atlas.provenance;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.resolver.ClusterResolver;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;

public class StandardAnalysisContext implements AnalysisContext {

    private final Logger logger = LoggerFactory.getLogger(StandardAnalysisContext.class);
    private final NiFiFlow nifiFlow;
    private final ClusterResolver clusterResolver;
    private final ProvenanceRepository provenanceRepository;

    public StandardAnalysisContext(NiFiFlow nifiFlow, ClusterResolver clusterResolver,
                                   ProvenanceRepository provenanceRepository) {
        this.nifiFlow = nifiFlow;
        this.clusterResolver = clusterResolver;
        this.provenanceRepository = provenanceRepository;
    }

    @Override
    public String lookupInputPortName(String componentId) {
        final AtlasObjectId portId = new AtlasObjectId(TYPE_NIFI_INPUT_PORT, ATTR_QUALIFIED_NAME, componentId);

        AtlasEntity ent = nifiFlow.getRootInputPortEntities().get(portId);
        return (String) ent.getAttribute("name");
    }

    @Override
    public String lookupOutputPortName(String componentId) {
        final AtlasObjectId portId = new AtlasObjectId(TYPE_NIFI_OUTPUT_PORT, ATTR_QUALIFIED_NAME, componentId);

        AtlasEntity ent = nifiFlow.getRootOutputPortEntities().get(portId);
        return (String) ent.getAttribute("name");
    }


    @Override
    public List<ConnectionStatus> findConnectionTo(String componentId) {
        return nifiFlow.getIncomingRelationShips(componentId);
    }

    @Override
    public List<ConnectionStatus> findConnectionFrom(String componentId) {
        return nifiFlow.getOutgoingRelationShips(componentId);
    }

    @Override
    public ClusterResolver getClusterResolver() {
        return clusterResolver;
    }

    @Override
    public ComputeLineageResult queryLineage(long eventId) {
        final ComputeLineageSubmission submission = provenanceRepository.submitLineageComputation(eventId, NIFI_USER);
        final ComputeLineageResult result = submission.getResult();
        try {
            if (result.awaitCompletion(10, TimeUnit.SECONDS)) {
                return result;
            }
            logger.warn("Lineage query for {} timed out.", new Object[]{eventId});
        } catch (InterruptedException e) {
            logger.warn("Lineage query for {} was interrupted due to {}.", new Object[]{eventId, e}, e);
        } finally {
            submission.cancel();
        }

        return null;
    }

    // TODO: Work around. User is required to avoid NullPointerException at PersistentProvenanceRepository.submitLineageComputation
    private static final QueryNiFiUser NIFI_USER = new QueryNiFiUser();
    private static class QueryNiFiUser implements NiFiUser {
        @Override
        public String getIdentity() {
            return StandardAnalysisContext.class.getSimpleName();
        }

        @Override
        public Set<String> getGroups() {
            return Collections.emptySet();
        }

        @Override
        public NiFiUser getChain() {
            return null;
        }

        @Override
        public boolean isAnonymous() {
            return true;
        }

        @Override
        public String getClientAddress() {
            return null;
        }
    }

    @Override
    public ProvenanceEventRecord getProvenanceEvent(long eventId) {
        try {
            return provenanceRepository.getEvent(eventId);
        } catch (IOException e) {
            logger.error("Failed to get provenance event for {} due to {}", new Object[]{eventId, e}, e);
            return null;
        }
    }
}
