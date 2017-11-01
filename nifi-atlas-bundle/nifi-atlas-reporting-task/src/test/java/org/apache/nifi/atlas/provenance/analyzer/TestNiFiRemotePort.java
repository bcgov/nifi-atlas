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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.atlas.resolver.ClusterResolvers;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.LineageEdge;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.lineage.LineageNodeType;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;
import static org.apache.nifi.provenance.lineage.LineageNodeType.FLOWFILE_NODE;
import static org.apache.nifi.provenance.lineage.LineageNodeType.PROVENANCE_EVENT_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.when;

public class TestNiFiRemotePort {

    @Test
    public void testRemoteInputPort() {
        final String componentType = "Remote Input Port";
        final String transitUri = "http://0.example.com:8080/nifi-api/data-transfer/input-ports/port-guid/transactions/tx-guid/flow-files";
        final ProvenanceEventRecord sendEvent = Mockito.mock(ProvenanceEventRecord.class);
        when(sendEvent.getEventId()).thenReturn(123L);
        when(sendEvent.getComponentId()).thenReturn("port-guid");
        when(sendEvent.getFlowFileUuid()).thenReturn("file-guid");
        when(sendEvent.getComponentType()).thenReturn(componentType);
        when(sendEvent.getTransitUri()).thenReturn(transitUri);
        when(sendEvent.getEventType()).thenReturn(ProvenanceEventType.SEND);

        final ProvenanceEventRecord createEvent = Mockito.mock(ProvenanceEventRecord.class);
        when(createEvent.getEventId()).thenReturn(456L);
        when(createEvent.getComponentId()).thenReturn("processor-guid");
        when(createEvent.getComponentType()).thenReturn("GenerateFlowFile");
        when(createEvent.getEventType()).thenReturn(ProvenanceEventType.CREATE);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostname(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final List<ConnectionStatus> connections = new ArrayList<>();
        final ConnectionStatus connection = new ConnectionStatus();
        connection.setDestinationId("port-guid");
        connection.setDestinationName("inputPortA");
        connections.add(connection);

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);
        when(context.findConnectionTo(matches("port-guid"))).thenReturn(connections);
        when(context.getProvenanceEvent(eq(456L))).thenReturn(createEvent);

        final ComputeLineageResult lineage = Mockito.mock(ComputeLineageResult.class);
        when(context.queryLineage(eq(123L))).thenReturn(lineage);

        final LineageNode sendEventNode = createLineageNode(PROVENANCE_EVENT_NODE, "123");
        final LineageNode flowFileNode = createLineageNode(FLOWFILE_NODE, "flowfile-uuid-1234");
        final LineageNode createEventNode = createLineageNode(PROVENANCE_EVENT_NODE, "456");

        final List<LineageEdge> edges = new ArrayList<>();
        edges.add(createLineageEdge(createEventNode, flowFileNode));
        edges.add(createLineageEdge(flowFileNode, sendEventNode));
        when(lineage.getEdges()).thenReturn(edges);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(componentType, transitUri, sendEvent.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, sendEvent);
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());
        assertEquals(1, refs.getComponentIds().size());
        // Should report connected componentId.
        assertTrue(refs.getComponentIds().contains("processor-guid"));

        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals(TYPE_NIFI_INPUT_PORT, ref.getTypeName());
        assertEquals("inputPortA", ref.get(ATTR_NAME));
        assertEquals("file-guid", ref.get(ATTR_QUALIFIED_NAME));
    }

    private LineageNode createLineageNode(LineageNodeType type, String id) {
        final LineageNode node = Mockito.mock(LineageNode.class);
        when(node.getNodeType()).thenReturn(type);
        when(node.getIdentifier()).thenReturn(id);
        return node;
    }

    private LineageEdge createLineageEdge(LineageNode from, LineageNode to) {
        final LineageEdge edge = Mockito.mock(LineageEdge.class);
        when(edge.getSource()).thenReturn(from);
        when(edge.getDestination()).thenReturn(to);
        return edge;
    }

    @Test
    public void testRemoteOutputPort() {
        // TODO: add test multiple connections.
        final String componentType = "Remote Output Port";
        final String transitUri = "http://0.example.com:8080/nifi-api/data-transfer/output-ports/port-guid/transactions/tx-guid/flow-files";
        final String sourceSystemFlowFileIdentifier = "urn:nifi:7ce27bc3-b128-4128-aba2-3a366435fd05";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentId()).thenReturn("port-guid");
        when(record.getComponentType()).thenReturn(componentType);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getFlowFileUuid()).thenReturn("file-guid");
        when(record.getSourceSystemFlowFileIdentifier()).thenReturn(sourceSystemFlowFileIdentifier);
        when(record.getEventType()).thenReturn(ProvenanceEventType.RECEIVE);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostname(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final List<ConnectionStatus> connections = new ArrayList<>();
        final ConnectionStatus connection = new ConnectionStatus();
        connection.setSourceId("port-guid");
        connection.setSourceName("outputPortA");
        connections.add(connection);

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);
        when(context.findConnectionFrom(matches("port-guid"))).thenReturn(connections);
        when(context.lookupOutputPortName("port-guid")).thenReturn("outputPortA");

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(componentType, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(1, refs.getInputs().size());
        assertEquals(0, refs.getOutputs().size());
        Referenceable ref = refs.getInputs().iterator().next();
        assertEquals(TYPE_NIFI_OUTPUT_PORT, ref.getTypeName());
        assertEquals("outputPortA", ref.get(ATTR_NAME));
        assertEquals("7ce27bc3-b128-4128-aba2-3a366435fd05", ref.get(ATTR_QUALIFIED_NAME));
    }


}
