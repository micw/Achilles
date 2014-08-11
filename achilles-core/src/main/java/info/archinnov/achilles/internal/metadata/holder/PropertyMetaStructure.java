package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.type.ConsistencyLevel;

public class PropertyMetaStructure extends PropertyMetaView {

    protected PropertyMetaStructure(PropertyMeta meta) {
        super(meta);
    }

    public boolean isEmbeddedId() {
        return meta.type.isEmbeddedId();
    }

    public boolean isClustered() {
        if (isEmbeddedId()) {
            return meta.embeddedIdProperties.isClustered();
        }
        return false;
    }

    public boolean isCounter() {
        return meta.type.isCounter();
    }

    public boolean isIndexed() {
        return meta.indexProperties != null;
    }

    public boolean isCollectionAndMap() {
        return meta.type.isCollectionAndMap();
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return meta.consistencyLevels != null ? meta.consistencyLevels.left : null;
    }


    public ConsistencyLevel getWriteConsistencyLevel() {
        return meta.consistencyLevels != null ? meta.consistencyLevels.right : null;
    }

}
