package org.apache.nifi.atlas;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.junit.Test;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

public class TestNiFIAtlasHook {

    @Test
    public void test() throws Exception {
        final NiFIAtlasHook hook = new NiFIAtlasHook();
//        hook.sendMessage();
//        hook.createLineageFromKafkaTopic();
        hook.createLineageToKafkaTopic();
    }

    @Test
    public void testNiFiFlowPath() throws Exception {

        final NiFIAtlasHook hook = new NiFIAtlasHook();

        final DataSetRefs refs = new DataSetRefs("03eb0e87-015e-1000-0000-00007e0d56ea");
        final Referenceable topic = new Referenceable("kafka_topic");
        topic.set(ATTR_NAME, "notification");
        topic.set("topic", "notification");
        topic.set(ATTR_QUALIFIED_NAME, "notification@HDPF");
        topic.set(ATTR_DESCRIPTION, "Description");
        topic.set("uri", "0.hdpf.aws.mine");
        refs.addInput(topic);

        final Referenceable flowRef = new Referenceable(TYPE_NIFI_FLOW);
        flowRef.set(ATTR_NAME, "NiFi Flow");
        flowRef.set(ATTR_QUALIFIED_NAME, "7c84501d-d10c-407c-b9f3-1d80e38fe36a ");
        flowRef.set(ATTR_URL, "http://0.hdpf.aws.mine:9990/");

        final Referenceable flowPathRef = new Referenceable(TYPE_NIFI_FLOW_PATH);
        flowPathRef.set(ATTR_NAME, "ConsumeKafka");
        flowPathRef.set(ATTR_QUALIFIED_NAME, "03eb0e87-015e-1000-0000-00007e0d56ea");
        flowPathRef.set(ATTR_NIFI_FLOW, flowRef);
        flowPathRef.set(ATTR_URL, "http://0.hdpf.aws.mine:9990/");


        hook.addDataSetRefs(refs, flowPathRef);
        hook.commitMessages();
    }
}
