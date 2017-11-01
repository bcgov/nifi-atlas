package org.apache.nifi.atlas.provenance;

import org.apache.atlas.typesystem.Referenceable;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class DataSetRefs {
    private final Set<String> componentIds;
    private Set<Referenceable> inputs;
    private Set<Referenceable> outputs;

    public DataSetRefs(String componentId) {
        this.componentIds = Collections.singleton(componentId);
    }

    public DataSetRefs(Set<String> componentIds) {
        this.componentIds = componentIds;
    }

    public Set<String> getComponentIds() {
        return componentIds;
    }

    public Set<Referenceable> getInputs() {
        return inputs != null ? inputs : Collections.emptySet();
    }

    public void addInput(Referenceable input) {
        if (inputs == null) {
            inputs = new LinkedHashSet<>();
        }
        inputs.add(input);
    }

    public Set<Referenceable> getOutputs() {
        return outputs != null ? outputs : Collections.emptySet();
    }

    public void addOutput(Referenceable output) {
        if (outputs == null) {
            outputs = new LinkedHashSet<>();
        }
        outputs.add(output);
    }

    public boolean isEmpty() {
        return (inputs == null || inputs.isEmpty()) && (outputs == null || outputs.isEmpty());
    }
}
