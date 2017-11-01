package org.apache.nifi.atlas;

import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.DataSetRefs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

/**
 * This class is not thread-safe as it holds uncommitted notification messages within instance.
 * {@link #addDataSetRefs(DataSetRefs, Referenceable, boolean)} and {@link #commitMessages()} should be used serially from a single thread.
 */
public class NiFIAtlasHook extends AtlasHook {

    private static final String CONF_PREFIX = "atlas.hook.nifi.";
    private static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }

    public void sendMessage() throws Exception {
        final List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();
        final Referenceable flow = new Referenceable(TYPE_NIFI_FLOW);
        flow.set(ATTR_NAME, "ut");
        flow.set(ATTR_QUALIFIED_NAME, "ut");
        flow.set(ATTR_DESCRIPTION, "Description");
        flow.set(ATTR_URL, "http://localhost:8080/nifi");

        final Referenceable path1 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path1.set(ATTR_NAME, "path1");
        path1.set(ATTR_QUALIFIED_NAME, "path1");
        path1.set(ATTR_DESCRIPTION, "Description");
        path1.set(ATTR_NIFI_FLOW, flow);

        final Referenceable path2 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path2.set(ATTR_NAME, "path2");
        path2.set(ATTR_QUALIFIED_NAME, "path2");
        path2.set(ATTR_DESCRIPTION, "Description");
        path2.set(ATTR_NIFI_FLOW, flow);

        final Referenceable path3 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path3.set(ATTR_NAME, "path3");
        path3.set(ATTR_QUALIFIED_NAME, "path3");
        path3.set(ATTR_DESCRIPTION, "Description");
        path3.set(ATTR_NIFI_FLOW, flow);

        final Referenceable path4 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path4.set(ATTR_NAME, "path4");
        path4.set(ATTR_QUALIFIED_NAME, "path4");
        path4.set(ATTR_DESCRIPTION, "Description");
        path4.set(ATTR_NIFI_FLOW, flow);

        final Referenceable path5 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path5.set(ATTR_NAME, "path5");
        path5.set(ATTR_QUALIFIED_NAME, "path5");
        path5.set(ATTR_DESCRIPTION, "Description");
        path5.set(ATTR_NIFI_FLOW, flow);

        final ArrayList<Object> path1Outputs = new ArrayList<>();
        path1Outputs.add(new Referenceable(path2));
        path1.set(ATTR_OUTPUTS, path1Outputs);

        final ArrayList<Object> path2Inputs = new ArrayList<>();
        path2Inputs.add(new Referenceable(path1));
        path2.set(ATTR_INPUTS, path2Inputs);

        final ArrayList<Object> path2Outputs = new ArrayList<>();
        path2Outputs.add(new Referenceable(path3));
        path2Outputs.add(new Referenceable(path4));
        path2.set(ATTR_OUTPUTS, path2Outputs);

        final ArrayList<Object> path3Inputs = new ArrayList<>();
        path3Inputs.add(new Referenceable(path2));
        path3.set(ATTR_INPUTS, path3Inputs);

        final ArrayList<Object> path3Outputs = new ArrayList<>();
        path3Outputs.add(new Referenceable(path4));
        path3.set(ATTR_OUTPUTS, path3Outputs);

        final ArrayList<Object> path4Inputs = new ArrayList<>();
        path4Inputs.add(new Referenceable(path3));
        path4.set(ATTR_INPUTS, path4Inputs);

        final ArrayList<Object> path4Outputs = new ArrayList<>();
        path4Outputs.add(new Referenceable(path5));
        path4.set(ATTR_OUTPUTS, path4Outputs);

        final ArrayList<Object> path5Inputs = new ArrayList<>();
        path5Inputs.add(new Referenceable(path4));
        path5.set(ATTR_INPUTS, path5Inputs);

        //        final AtlasObjectId nifiFlowId = new AtlasObjectId(TYPE_NIFI_FLOW, ATTR_QUALIFIED_NAME, "7c84501d-d10c-407c-b9f3-1d80e38fe36a");
//        final Referenceable nifiFlowRef = new Referenceable(TYPE_NIFI_FLOW);
//        nifiFlowRef.set("name", "NiFi Flow");
//        nifiFlowRef.set("qualifiedName", "7c84501d-d10c-407c-b9f3-1d80e38fe36a");

        // TODO: how can I set nifi_flow reference?? If I use Referenceable, it requires all mandatory attributes.
        // TODO: If I use AtlasObjectId, serialization error occurs
        // TODO: If I know its id and version, I can use Id.
//        ref.set("nifiFlow", nifiFlowRef);
//        final Id nifiFlowId = new Id("6b62d74f-0650-4dbb-af4f-be460a8621c5", 1, TYPE_NIFI_FLOW);
//        path1.set("nifiFlow", nifiFlowId);
//
//        final Id p1RefId = new Id("689721a6-b1de-491b-90ca-c0f746c4ee21", 1, TYPE_NIFI_FLOW_PATH);
//        final Id p3RefId = new Id("a46273c4-0211-4202-9c50-5830b54bbe22", 1, TYPE_NIFI_FLOW_PATH);
//        final Id p5RefId = new Id("7bacf1ca-6220-4b7b-9072-d4b08ffbf5d2", 1, TYPE_NIFI_FLOW_PATH);
//        final Id queueRefId = new Id("4925bd7e-b656-494a-8ba2-55538f0968e1", 1, TYPE_NIFI_QUEUE);
//        final ArrayList<Id> inputs = new ArrayList<>();
//        inputs.add(p1RefId);
////        inputs.add(p3RefId);
//        final ArrayList<Id> outputs = new ArrayList<>();
//        outputs.add(queueRefId);
//        path1.set("inputs", inputs);
//        path1.set("outputs", outputs);


        final HookNotification.EntityCreateRequest message = new HookNotification.EntityCreateRequest("nifi",
//                flow, path5, path4, path3, path2, path1);
//                flow, path1, path2, path3, path4);
                path2);
        messages.add(message);
        notifyEntities(messages);
    }

    public void createLineageFromKafkaTopic() throws Exception {
        final List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();
        final Referenceable topic = new Referenceable("kafka_topic");
        topic.set(ATTR_NAME, "trucking-data");
        topic.set("topic", "trucking-data");
        topic.set(ATTR_QUALIFIED_NAME, "trucking-data@HDPF");
        topic.set(ATTR_DESCRIPTION, "Description");
        topic.set("uri", "0.hdpf.aws.mine");
        final HookNotification.EntityCreateRequest createTopic = new HookNotification.EntityCreateRequest("nifi", topic);


        final Referenceable path1 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path1.set(ATTR_QUALIFIED_NAME, "path1");
        final ArrayList<Object> path1Inputs = new ArrayList<>();
        path1Inputs.add(new Referenceable(topic));
        path1.set(ATTR_INPUTS, path1Inputs);

        messages.add(createTopic);
        messages.add(new HookNotification.EntityPartialUpdateRequest("nifi", TYPE_NIFI_FLOW_PATH, ATTR_QUALIFIED_NAME, "path1", path1));
        notifyEntities(messages);

    }

    public void createLineageToKafkaTopic() throws Exception {
        final List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();
        final Referenceable topic = new Referenceable("kafka_topic");
        topic.set(ATTR_NAME, "notification");
        topic.set("topic", "notification");
        topic.set(ATTR_QUALIFIED_NAME, "notification@HDPF");
        topic.set(ATTR_DESCRIPTION, "Description");
        topic.set("uri", "0.hdpf.aws.mine");
        final HookNotification.EntityCreateRequest createTopic = new HookNotification.EntityCreateRequest("nifi", topic);


        final Referenceable path5 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path5.set(ATTR_QUALIFIED_NAME, "path5");
        final ArrayList<Object> path5Outputs = new ArrayList<>();
        path5Outputs.add(new Referenceable(topic));
        path5.set(ATTR_OUTPUTS, path5Outputs);

        messages.add(createTopic);
        messages.add(new HookNotification.EntityPartialUpdateRequest("nifi", TYPE_NIFI_FLOW_PATH, ATTR_QUALIFIED_NAME, "path5", path5));
        notifyEntities(messages);

    }

    private static final String NIFI_USER = "nifi";

    private final List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private void addDataSetRefs(Set<Referenceable> dataSetRefs, Referenceable nifiFlowPath, String targetAttribute) {
        if (dataSetRefs != null && !dataSetRefs.isEmpty()) {
            for (Referenceable dataSetRef : dataSetRefs) {
                final HookNotification.EntityCreateRequest createDataSet = new HookNotification.EntityCreateRequest(NIFI_USER, dataSetRef);
                messages.add(createDataSet);
            }

            Object updatedRef = nifiFlowPath.get(targetAttribute);
            if (updatedRef == null) {
                updatedRef = new ArrayList(dataSetRefs);
            } else {
                ((Collection<Referenceable>) updatedRef).addAll(dataSetRefs);
            }
            nifiFlowPath.set(targetAttribute, updatedRef);
        }
    }

    public void addDataSetRefs(DataSetRefs dataSetRefs, Referenceable flowPathRef) {
        addDataSetRefs(dataSetRefs, flowPathRef, false);
    }

    public void addDataSetRefs(DataSetRefs dataSetRefs, Referenceable flowPathRef, boolean create) {
        addDataSetRefs(dataSetRefs.getInputs(), flowPathRef, ATTR_INPUTS);
        addDataSetRefs(dataSetRefs.getOutputs(), flowPathRef, ATTR_OUTPUTS);
        // Here, EntityPartialUpdateRequest adds Process's inputs or outputs elements who does not exists in
        // the current nifi_flow_path entity stored in Atlas.

        if (create) {
            messages.add(new HookNotification.EntityCreateRequest(NIFI_USER, flowPathRef));
        } else {
            messages.add(new HookNotification.EntityPartialUpdateRequest(NIFI_USER, TYPE_NIFI_FLOW_PATH,
                    ATTR_QUALIFIED_NAME, (String) flowPathRef.get(ATTR_QUALIFIED_NAME), flowPathRef));
        }
    }

    public void addCreateReferenceable (Collection<Referenceable> ins, Referenceable ref) {
        if (ins != null && !ins.isEmpty()) {
            for (Referenceable dataSetRef : ins) {
                final HookNotification.EntityCreateRequest createDataSet = new HookNotification.EntityCreateRequest(NIFI_USER, dataSetRef);
                messages.add(createDataSet);
            }
        }
        messages.add(new HookNotification.EntityCreateRequest(NIFI_USER, ref));
    }

    public void addUpdateReferenceable (Referenceable ref) {
        messages.add(new HookNotification.EntityPartialUpdateRequest(NIFI_USER, ref.getTypeName(),
                ATTR_QUALIFIED_NAME, (String) ref.get(ATTR_QUALIFIED_NAME), ref));
    }

    public void commitMessages() {
        try {
            notifyEntities(messages);
        } finally {
            messages.clear();
        }
    }
}
