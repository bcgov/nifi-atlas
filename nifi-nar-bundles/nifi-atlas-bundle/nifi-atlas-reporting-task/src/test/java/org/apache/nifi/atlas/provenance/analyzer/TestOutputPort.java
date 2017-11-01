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
import static org.apache.nifi.provenance.lineage.LineageNodeType.FLOWFILE_NODE;
import static org.apache.nifi.provenance.lineage.LineageNodeType.PROVENANCE_EVENT_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.when;

public class TestOutputPort {

    @Test
    public void testOutputPort() {
        final String processorName = "Output Port";

        ConnectionStatus con1 = Mockito.mock(ConnectionStatus.class);
        when(con1.getSourceId()).thenReturn("101");

        List<ConnectionStatus> connectionStatuses = new ArrayList<>();
        connectionStatuses.add(con1);

        final ProvenanceEventRecord sendEvent = Mockito.mock(ProvenanceEventRecord.class);
        when(sendEvent.getEventId()).thenReturn(456L);
        when(sendEvent.getComponentId()).thenReturn("processor-guid");
        when(sendEvent.getComponentType()).thenReturn("PutFile");
        when(sendEvent.getEventType()).thenReturn(ProvenanceEventType.SEND);

        final String componentId = "100";
        final String transitUri = "nifi://174.138.41.167:5181/f4a90e5d-0f64-4d5a-ad7d-5f0c6c7dc48e";

        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getEventId()).thenReturn(123L);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getComponentId()).thenReturn(componentId);
        when(record.getFlowFileUuid()).thenReturn("1-2-3-4");
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostname(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);
        when(context.lookupOutputPortName(componentId)).thenReturn("OUT_PORT");
        when(context.findConnectionTo(componentId)).thenReturn(connectionStatuses);
        when(context.getProvenanceEvent(eq(456L))).thenReturn(sendEvent);

        final ComputeLineageResult lineage = Mockito.mock(ComputeLineageResult.class);
        when(context.queryLineage(eq(123L))).thenReturn(lineage);

        final LineageNode sendEventNode = createLineageNode(PROVENANCE_EVENT_NODE, "123");
        final LineageNode flowFileNode = createLineageNode(FLOWFILE_NODE, "flowfile-uuid-1234");
        final LineageNode createEventNode = createLineageNode(PROVENANCE_EVENT_NODE, "456");

        final List<LineageEdge> edges = new ArrayList<>();
        edges.add(createLineageEdge(createEventNode, flowFileNode));
        edges.add(createLineageEdge(flowFileNode, sendEventNode));
        when(lineage.getEdges()).thenReturn(edges);


        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(1, refs.getComponentIds().size());
        assertEquals("processor-guid", refs.getComponentIds().iterator().next());
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("nifi_output_port", ref.getTypeName());
        assertEquals("OUT_PORT", ref.get(ATTR_NAME));
        assertEquals("1-2-3-4", ref.get(ATTR_QUALIFIED_NAME));
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

}
