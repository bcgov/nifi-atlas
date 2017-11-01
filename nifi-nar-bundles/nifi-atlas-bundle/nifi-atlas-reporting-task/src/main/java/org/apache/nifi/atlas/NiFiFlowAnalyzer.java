/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.reporting.ReportingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;

public class NiFiFlowAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(NiFiFlowAnalyzer.class);

    public NiFiFlow analyzeProcessGroup(AtlasVariables atlasVariables, ReportingContext context) throws IOException {
        final ProcessGroupStatus rootProcessGroup = context.getEventAccess().getGroupStatus("root");

        final String flowName = rootProcessGroup.getName();
        // TODO: improve this.
        final String nifiUrlForAtlasMetadata = atlasVariables.getNifiUrl();
        final String nifiUrl = nifiUrlForAtlasMetadata;
        final NiFiFlow nifiFlow = new NiFiFlow(flowName, rootProcessGroup.getId(), nifiUrl);

        // TODO: Do we have anything to set?
        // nifiFlow.setDescription();

        analyzeProcessGroup(rootProcessGroup, nifiFlow);

        analyzeRootGroupPorts(nifiFlow, rootProcessGroup);

        return nifiFlow;
    }

    private void analyzeRootGroupPorts(NiFiFlow nifiFlow, ProcessGroupStatus rootProcessGroup) {
        BiConsumer<PortStatus, Boolean> portEntityCreator = (port, isInput) -> {
            final String typeName = isInput ? TYPE_NIFI_INPUT_PORT : TYPE_NIFI_OUTPUT_PORT;

            final AtlasEntity entity = new AtlasEntity(typeName);
            final String portName = port.getName();

            entity.setAttribute(ATTR_NIFI_FLOW, nifiFlow.getId());
            entity.setAttribute(ATTR_NAME, portName);
            entity.setAttribute(ATTR_QUALIFIED_NAME, port.getId());
            // TODO: do we have anything to set?
//            entity.setAttribute(ATTR_DESCRIPTION, port.getComponent().getComments());

            final AtlasObjectId portId = new AtlasObjectId(typeName, ATTR_QUALIFIED_NAME, port.getId());
            final Map<AtlasObjectId, AtlasEntity> ports = isInput ? nifiFlow.getRootInputPortEntities() : nifiFlow.getRootOutputPortEntities();
            ports.put(portId, entity);

            if (isInput) {
                nifiFlow.addRootInputPort(port);
            } else {
                nifiFlow.addRootOutputPort(port);
            }
        };

        rootProcessGroup.getInputPortStatus().forEach(port -> portEntityCreator.accept(port, true));
        rootProcessGroup.getOutputPortStatus().forEach(port -> portEntityCreator.accept(port, false));
    }

    private void analyzeProcessGroup(final ProcessGroupStatus processGroupStatus, final NiFiFlow nifiFlow) throws IOException {

        processGroupStatus.getConnectionStatus().forEach(c -> nifiFlow.addConnection(c));
        processGroupStatus.getProcessorStatus().forEach(p -> nifiFlow.addProcessor(p));
        processGroupStatus.getRemoteProcessGroupStatus().forEach(r -> nifiFlow.addRemoteProcessGroup(r));
        processGroupStatus.getInputPortStatus().forEach(p -> nifiFlow.addInputPort(p));
        processGroupStatus.getOutputPortStatus().forEach(p -> nifiFlow.addOutputPort(p));

        // Analyze child ProcessGroups recursively.
        for (ProcessGroupStatus child : processGroupStatus.getProcessGroupStatus()) {
            analyzeProcessGroup(child, nifiFlow);
        }

    }

    private List<String> getIncomingProcessorsIds(NiFiFlow nifiFlow, List<ConnectionStatus> incomingConnections) {
        if (incomingConnections == null) {
            return Collections.emptyList();
        }

        final List<String> ids = new ArrayList<>();

        incomingConnections.forEach(c -> {
            // Ignore self relationship.
            final String sourceId = c.getSourceId();
            if (!sourceId.equals(c.getDestinationId())) {
                if (nifiFlow.getProcessors().containsKey(sourceId)) {
                    ids.add(sourceId);
                } else {
                    ids.addAll(getIncomingProcessorsIds(nifiFlow, nifiFlow.getIncomingRelationShips(sourceId)));
                }
            }
        });

        return ids;
    }

    private void traverse(NiFiFlow nifiFlow, List<NiFiFlowPath> paths, NiFiFlowPath path, String pid) {

        // Skipping non-processor outgoing relationships
        if (nifiFlow.getProcessors().containsKey(pid)) {
            path.addProcessor(pid);

            if (nifiFlow.getInputs(pid) != null) {
                path.getInputs().addAll(nifiFlow.getInputs(pid));
            }

            if (nifiFlow.getOutputs(pid) != null) {
                path.getOutputs().addAll(nifiFlow.getOutputs(pid));
            }
        }

        final List<ConnectionStatus> outs = nifiFlow.getOutgoingRelationShips(pid);
        if (outs == null) {
            return;
        }

        // Skip non-processor outputs.
        final Predicate<ConnectionStatus> isProcessor = c -> nifiFlow.getProcessors().containsKey(c.getDestinationId());
        outs.stream().filter(isProcessor.negate())
                .map(c -> c.getDestinationId())
                .forEach(destId -> traverse(nifiFlow, paths, path, destId));

        // Analyze destination processors.
        outs.stream().filter(isProcessor).forEach(out -> {
            final String destPid = out.getDestinationId();
            if (pid.equals(destPid)) {
                // Avoid loop.
                return;
            }

            // Count how many incoming relationship the destination has.
            long destIncomingConnectionCount = getIncomingProcessorsIds(nifiFlow, nifiFlow.getIncomingRelationShips(destPid)).size();

            if (destIncomingConnectionCount > 1) {
                // If destination has more than one (except the destination itself), it is an independent flow path.
                final NiFiFlowPath newJointPoint = new NiFiFlowPath(destPid);

                final boolean exists = paths.contains(newJointPoint);
                final NiFiFlowPath jointPoint = exists ? paths.stream()
                        .filter(p -> p.equals(newJointPoint)).findFirst().get() : newJointPoint;

                // Link together.
                path.getOutgoingPaths().add(jointPoint);
                jointPoint.getIncomingPaths().add(path);

                if (exists) {
                    // Link existing incoming queue of the joint point.
                    path.getOutputs().add(jointPoint.getInputs().iterator().next());

                } else {
                    // Add jointPoint only if it doesn't exist, to avoid adding the same jointPoint again.
                    paths.add(jointPoint);

                    // Create an input queue DataSet because Atlas doesn't show lineage if it doesn't have in and out.
                    // This DataSet is also useful to link flowPaths together on Atlas lineage graph.
                    final AtlasObjectId queueId = new AtlasObjectId(TYPE_NIFI_QUEUE, ATTR_QUALIFIED_NAME, destPid);

                    final AtlasEntity queue = new AtlasEntity(TYPE_NIFI_QUEUE);
                    queue.setAttribute(ATTR_NIFI_FLOW, nifiFlow.getId());
                    queue.setAttribute(ATTR_QUALIFIED_NAME, destPid);
                    queue.setAttribute(ATTR_NAME, "queue");
                    queue.setAttribute(ATTR_DESCRIPTION, "Input queue for " + destPid);

                    nifiFlow.getQueues().put(queueId, queue);
                    newJointPoint.getInputs().add(queueId);
                    path.getOutputs().add(queueId);

                    // Start traversing as a new joint point.
                    traverse(nifiFlow, paths, jointPoint, destPid);
                }

            } else {
                // Normal relation, continue digging.
                traverse(nifiFlow, paths, path, destPid);
            }

        });
    }

    private boolean isOnlyConnectedToRootInputPortOrNone(NiFiFlow nifiFlow, List<ConnectionStatus> ins) {
        if (ins == null || ins.isEmpty()) {
            return true;
        }
        return ins.stream().allMatch(
                in -> {
                    // If it has incoming relationship from other processor, then return false.
//                    final boolean isProcessor = sourceType.equals("PROCESSOR");
                    final String sourceId = in.getSourceId();
                    final boolean isProcessor = nifiFlow.isProcessor(sourceId);
                    // RemoteOutputPort does not have any further input.
                    // TODO: How can we know?
//                    final boolean isRemoteOutput = sourceType.equals("REMOTE_OUTPUT_PORT");
                    final boolean isRemoteOutput = false;
                    // Root Group InputPort does not have any further input.
                    final boolean isRootGroupInput = nifiFlow.isRootInputPort(sourceId);
                    final boolean checkNext = isOnlyConnectedToRootInputPortOrNone(nifiFlow,
                            nifiFlow.getIncomingRelationShips(sourceId));
                    return !isProcessor && (isRootGroupInput || isRemoteOutput || checkNext);
                }
        );
    }

    public void analyzePaths(NiFiFlow nifiFlow) {
        // Now let's break it into flow paths.
        // TODO: add tests that confirms various situations, Remote Ports, Funnel, Root Group Ports ... etc.
        final Set<String> headProcessors = nifiFlow.getProcessors().keySet().stream()
                .filter(pid -> {
                    final List<ConnectionStatus> ins = nifiFlow.getIncomingRelationShips(pid);
                    return isOnlyConnectedToRootInputPortOrNone(nifiFlow, ins);
                })
                .collect(Collectors.toSet());

        headProcessors.forEach(startPid -> {
            // TODO: Can we improve this a bit, if new processor is inserted, then entity id will be changed, and existing lineage to DataSet will be lost. But that may be OK.
            // By using the startPid as its qualifiedName, it's guaranteed that
            // the same path will end up being the same Atlas entity.
            final NiFiFlowPath path = new NiFiFlowPath(startPid);
            final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();
            paths.add(path);
            traverse(nifiFlow, paths, path, startPid);
        });
    }

}
