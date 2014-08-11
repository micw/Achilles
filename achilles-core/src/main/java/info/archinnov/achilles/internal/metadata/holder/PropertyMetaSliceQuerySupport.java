package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.internal.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;

public class PropertyMetaSliceQuerySupport extends PropertyMetaView {

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaSliceQuerySupport.class);

    protected PropertyMetaSliceQuerySupport(PropertyMeta meta) {
        super(meta);
    }

    List<String> getPartitionKeysName(int size) {
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot get {} partition key names for entity '%s' because it does not have a compound primary key", size, meta.entityClassName);
        return meta.embeddedIdProperties.getPartitionComponentNames().subList(0, size);
    }

    String getLastPartitionKeyName() {
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot get last partition key name for entity '%s' because it does not have a compound primary key", meta.entityClassName);
        return meta.embeddedIdProperties.getLastPartitionKeyName();
    }

    List<String> getClusteringKeysName(int size) {
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot get {} clustering key names for entity '%s' because it does not have a compound primary key",size, meta.entityClassName);
        return meta.embeddedIdProperties.getClusteringComponentNames().subList(0, size);
    }

    String getLastClusteringKeyName() {
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot get last clustering key name for entity '%s' because it does not have a compound primary key",meta.entityClassName);
        return meta.embeddedIdProperties.getLastClusteringKeyName();
    }

    int getPartitionKeysSize() {
        return meta.embeddedIdProperties.getPartitionKeysSize();
    }

    int getClusteringKeysSize() {
        return meta.embeddedIdProperties.getClusteringKeysSize();
    }

    void validatePartitionComponents(Object...partitionComponents) {
        log.trace("Validate partition key components {} for entity {}", partitionComponents, meta.entityClassName);
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot validate partition components for entity '%s' because it does not have a compound primary key",meta.entityClassName);
        meta.embeddedIdProperties.validatePartitionComponents(meta.entityClassName, partitionComponents);
    }

    void validatePartitionComponentsIn(Object...partitionComponentsIN) {
        log.trace("Validate partition key components IN {} for entity {}", partitionComponentsIN, meta.entityClassName);
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot validate partition components IN for entity '%s' because it does not have a compound primary key",meta.entityClassName);
        meta.embeddedIdProperties.validatePartitionComponentsIn(meta.entityClassName, partitionComponentsIN);
    }

    void validateClusteringComponents(Object...clusteringKeys) {
        log.trace("Validate clustering keys {} for entity {}", clusteringKeys, meta.entityClassName);
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot validate clustering keys for entity '%s' because it does not have a compound primary key",meta.entityClassName);
        meta.embeddedIdProperties.validateClusteringComponents(meta.entityClassName, clusteringKeys);
    }

    void validateClusteringComponentsIn(Object...clusteringKeysIN) {
        log.trace("Validate clustering keys IN {} for entity {}", clusteringKeysIN, meta.entityClassName);
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot validate clustering keys IN for entity '%s' because it does not have a compound primary key",meta.entityClassName);
        meta.embeddedIdProperties.validateClusteringComponentsIn(meta.entityClassName, clusteringKeysIN);
    }

    ClusteringOrder getClusteringOrder() {
        Validator.validateTrue(meta.structure().isClustered(),"Cannot get clustering order for entity {} because it is not clustered", meta.entityClassName);
        return meta.embeddedIdProperties.getClusteringOrders().get(0);
    }

    List<Object> encodePartitionComponents(List<Object> rawPartitionComponents) {
        Validator.validateTrue(meta.type == PropertyType.EMBEDDED_ID, "Cannot encode partition components '%s' for the property '%s' which is not a compound primary key", rawPartitionComponents, meta.propertyName);
        return meta.embeddedIdProperties.encodePartitionComponents(rawPartitionComponents);
    }

    List<Object> encodePartitionComponentsIN(List<Object> rawPartitionComponentsIN) {
        Validator.validateTrue(meta.type == PropertyType.EMBEDDED_ID, "Cannot encode partition components '%s' for the property '%s' which is not a compound primary key", rawPartitionComponentsIN, meta.propertyName);
        return meta.embeddedIdProperties.encodePartitionComponentsIN(rawPartitionComponentsIN);
    }

    List<Object> encodeClusteringKeys(List<Object> rawClusteringKeys) {
        Validator.validateTrue(meta.type == PropertyType.EMBEDDED_ID, "Cannot encode clustering components '%s' for the property '%s' which is not a compound primary key", rawClusteringKeys, meta.propertyName);
        return meta.embeddedIdProperties.encodeClusteringKeys(rawClusteringKeys);
    }

    List<Object> encodeClusteringKeysIN(List<Object> rawClusteringKeysIN) {
        Validator.validateTrue(meta.type == PropertyType.EMBEDDED_ID, "Cannot encode clustering components '%s' for the property '%s' which is not a compound primary key", rawClusteringKeysIN, meta.propertyName);
        return meta.embeddedIdProperties.encodeClusteringKeysIN(rawClusteringKeysIN);
    }
}
