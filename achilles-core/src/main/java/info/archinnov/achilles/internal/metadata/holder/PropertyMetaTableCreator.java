package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.schemabuilder.Create;

import java.util.List;

import static info.archinnov.achilles.schemabuilder.SchemaBuilder.createIndex;
import static org.apache.commons.lang.StringUtils.isBlank;

public class PropertyMetaTableCreator extends PropertyMetaView {

    protected PropertyMetaTableCreator(PropertyMeta meta) {
        super(meta);
    }

    public void addPartitionKeys(Create createTable) {
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot create partition components for entity '%s' because it does not have a compound primary key", meta.entityClassName);
        meta.embeddedIdProperties.addPartitionKeys(createTable);
    }

    public void addClusteringKeys(Create createTable) {
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot create clustering keys for entity '%s' because it does not have a compound primary key",meta.entityClassName);
        meta.embeddedIdProperties.addClusteringKeys(createTable);
    }

    public void createNewIndex(String tableName, List<String> indexes) {
        Validator.validateNotNull(meta.indexProperties, "Cannot create new index on property {} of entity {} because it is not defined as indexed",meta.propertyName, meta.entityClassName);
        indexes.add(createNewIndexScript(tableName));
    }

    public String createNewIndexScript(String tableName) {
        Validator.validateNotNull(meta.indexProperties, "Cannot create new index script on property {} of entity {} because it is not defined as indexed",meta.propertyName, meta.entityClassName);
        final String optionalIndexName = meta.indexProperties.getIndexName();
        final String indexName = isBlank(optionalIndexName) ? tableName + "_" + meta.getCQL3PropertyName() : optionalIndexName;
        return createIndex(indexName).onTable(tableName).andColumn(meta.getCQL3PropertyName());
    }

    public Create.Options addClusteringOrder(Create.Options tableOptions) {
        if (meta.structure().isClustered()) {
            final List<Create.Options.ClusteringOrder> clusteringOrders = meta.embeddedIdProperties.getClusteringOrders();
            return tableOptions.clusteringOrder(clusteringOrders.toArray(new Create.Options.ClusteringOrder[clusteringOrders.size()]));
        }
        return tableOptions;
    }


    public Class<?> getValueClassForTableCreationAndValidation() {
        if (meta.timeUUID) {
            return InternalTimeUUID.class;
        } else {
            return meta.valueClass;
        }
    }
}
