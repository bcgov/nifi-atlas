package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;

import java.net.URI;

import static org.apache.nifi.atlas.NiFiTypes.*;

public abstract class AbstractFileAnalyzer extends AbstractNiFiProvenanceEventAnalyzer {

    static final String TYPE_FILE = "fs_path";

    static final String ATTR_FILE_CREATE_TIME = "createTime";
    static final String ATTR_FILE_MODIFIED_TIME = "modifiedTime";
    static final String ATTR_FILE_SIZE = "fileSize";
    static final String ATTR_FILE_GROUP = "group";
    static final String ATTR_FILE_OWNER = "owner";
    static final String ATTR_IS_FILE = "isFile";

    protected Referenceable createFileRef(String clusterName, URI transitUrl) {
        final Referenceable ref = new Referenceable(TYPE_FILE);
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, transitUrl.getPath()));
        return ref;
    }

}
