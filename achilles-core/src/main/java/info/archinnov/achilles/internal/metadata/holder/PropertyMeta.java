/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.internal.persistence.operations.InternalCounterImpl;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

public class PropertyMeta {

    private static final Logger log = LoggerFactory.getLogger(PropertyMeta.class);

    public static final Predicate<PropertyMeta> STATIC_COLUMN_FILTER = new Predicate<PropertyMeta>() {
        @Override
        public boolean apply(PropertyMeta pm) {
            return pm.isStaticColumn();
        }
    };

    public static final Predicate<PropertyMeta> COUNTER_COLUMN_FILTER = new Predicate<PropertyMeta>() {
        @Override
        public boolean apply(PropertyMeta pm) {
            return pm.isCounter();
        }
    };


    private static final Function<String, String> TO_LOWER_CASE = new Function<String, String>() {

        @Override
        public String apply(String input) {
            String result = null;
            if (StringUtils.isNotBlank(input))
                result = input.toLowerCase();

            return result;
        }
    };

    private PropertyType type;
    private String propertyName;
    private String entityClassName;
    private Class<?> keyClass;
    private Class<?> valueClass;
    private Method getter;
    private Method setter;
    private Field field;
    private CounterProperties counterProperties;
    private EmbeddedIdProperties embeddedIdProperties;
    private IndexProperties indexProperties;
    private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
    private boolean timeUUID = false;
    private DataTranscoder transcoder;
    private boolean emptyCollectionAndMapIfNull = false;
    private boolean staticColumn = false;
    private ReflectionInvoker invoker = new ReflectionInvoker();

    public List<Field> getComponentFields() {
        log.trace("Get component fields");
        List<Field> compFields = new ArrayList<>();
        if (embeddedIdProperties != null) {
            compFields = embeddedIdProperties.getComponentFields();
        }
        return compFields;
    }

    public List<Method> getComponentGetters() {
        log.trace("Get component getters");
        List<Method> compGetters = new ArrayList<>();
        if (embeddedIdProperties != null) {
            compGetters = embeddedIdProperties.getComponentGetters();
        }
        return compGetters;
    }

    public Field getPartitionKeyField() {
        log.trace("Get partition key field");
        Field field = null;
        if (embeddedIdProperties != null) {
            field = embeddedIdProperties.getComponentFields().get(0);
        }
        return field;
    }

    public List<Method> getComponentSetters() {
        log.trace("Get component setters");
        List<Method> compSetters = new ArrayList<>();
        if (embeddedIdProperties != null) {
            compSetters = embeddedIdProperties.getComponentSetters();
        }
        return compSetters;
    }

    public List<Class<?>> getComponentClasses() {
        List<Class<?>> compClasses = new ArrayList<>();
        if (embeddedIdProperties != null) {
            compClasses = embeddedIdProperties.getComponentClasses();
        }
        return compClasses;
    }

    public List<String> getComponentNames() {
        log.trace("Get component classes");
        List<String> components = new ArrayList<>();
        if (embeddedIdProperties != null) {
            return embeddedIdProperties.getComponentNames();
        }
        return components;
    }

    public List<String> getCQLComponentNames() {
        log.trace("Get CQL component names");
        return FluentIterable.from(getComponentNames()).transform(TO_LOWER_CASE).toList();
    }

    public List<String> getClusteringComponentNames() {
        log.trace("Get clustering component names");
        return embeddedIdProperties != null ? embeddedIdProperties.getClusteringComponentNames() : Arrays
                .<String>asList();
    }

    public List<Class<?>> getClusteringComponentClasses() {
        log.trace("Get clustering component classes");
        return embeddedIdProperties != null ? embeddedIdProperties.getClusteringComponentClasses() : Arrays
                .<Class<?>>asList();
    }

    public List<String> getPartitionComponentNames() {
        log.trace("Get partition key component names");
        return embeddedIdProperties != null ? embeddedIdProperties.getPartitionComponentNames() : Arrays
                .<String>asList();
    }

    public List<Class<?>> getPartitionComponentClasses() {
        log.trace("Get partition key component classes");
        return embeddedIdProperties != null ? embeddedIdProperties.getPartitionComponentClasses() : Arrays
                .<Class<?>>asList(valueClass);
    }

    public void validatePartitionComponents(Object...partitionComponents) {
        log.trace("Validate partition key components");
        if (embeddedIdProperties != null) {
            embeddedIdProperties.validatePartitionComponents(this.entityClassName, partitionComponents);
        }
    }

    public void validatePartitionComponentsIn(Object...partitionComponents) {
        log.trace("Validate partition key components");
        if (embeddedIdProperties != null) {
            embeddedIdProperties.validatePartitionComponentsIn(this.entityClassName, partitionComponents);
        }
    }

    public void validateClusteringComponents(Object...clusteringComponents) {
        log.trace("Validate clustering components");
        if (embeddedIdProperties != null) {
            embeddedIdProperties.validateClusteringComponents(this.entityClassName, clusteringComponents);
        }
    }

    public void validateClusteringComponentsIn(Object...clusteringComponents) {
        log.trace("Validate clustering components");
        if (embeddedIdProperties != null) {
            embeddedIdProperties.validateClusteringComponentsIn(this.entityClassName, clusteringComponents);
        }
    }

    public List<Field> getPartitionComponentFields() {
        log.trace("Get partition key component fields");
        return embeddedIdProperties != null ? embeddedIdProperties.getPartitionComponentFields() : Arrays
                .<Field>asList();
    }

    public boolean isPrimaryKeyTimeUUID(String componentName) {
        log.trace("Determine whether component {} is of TimeUUID type", componentName);
        return embeddedIdProperties != null && embeddedIdProperties.getTimeUUIDComponents().contains(componentName);
    }

    public String getOrderingComponent() {
        log.trace("Get ordering component name");
        String component = null;
        if (embeddedIdProperties != null) {
            return embeddedIdProperties.getOrderingComponent();
        }
        return component;
    }

    public List<ClusteringOrder> getClusteringOrders() {
        log.trace("Get clustering orders if any");
        List<ClusteringOrder> clusteringOrders = new LinkedList<>();
        if (embeddedIdProperties != null) {
            return embeddedIdProperties.getCluseringOrders();
        }
        return clusteringOrders;
    }



    public PropertyMeta counterIdMeta() {
        return counterProperties != null ? counterProperties.getIdMeta() : null;
    }

    public String fqcn() {
        return counterProperties != null ? counterProperties.getFqcn() : null;
    }

    public boolean isCounter() {
        return this.type.isCounter();
    }

    public boolean isEmbeddedId() {
        return type.isEmbeddedId();
    }

    public boolean isClustered() {
        boolean isClustered = false;
        if (embeddedIdProperties != null) {
            isClustered = !embeddedIdProperties.getClusteringComponentClasses().isEmpty();
        }
        return isClustered;
    }

    public boolean isCollectionAndMap() {
        return type.isCollectionAndMap();
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return consistencyLevels != null ? consistencyLevels.left : null;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return consistencyLevels != null ? consistencyLevels.right : null;
    }

    public Object decode(Object cassandraValue) {
        return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
    }

    public Object decodeKey(Object cassandraValue) {
        return cassandraValue == null ? null : transcoder.decodeKey(this, cassandraValue);
    }

    public List<Object> decode(List<?> cassandraValue) {
        return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
    }

    public Set<Object> decode(Set<?> cassandraValue) {
        return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
    }

    public Map<Object, Object> decode(Map<?, ?> cassandraValue) {
        return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
    }

    public Object decodeFromComponents(List<?> components) {
        return components == null ? null : transcoder.decodeFromComponents(this, components);
    }

    public Object getAndEncodeValueForCassandra(Object entity) {
        Object value = getValueFromField(entity);
        Object encoded = null;
        if (value != null) {
            switch (type) {
                case SIMPLE:
                    encoded = transcoder.encode(this, value);
                    break;
                case LIST:
                    encoded = transcoder.encode(this, (List<?>) value);
                    break;
                case SET:
                    encoded = transcoder.encode(this, (Set<?>) value);
                    break;
                case MAP:
                    encoded = transcoder.encode(this, (Map<?, ?>) value);
                    break;
                case COUNTER:
                    encoded = ((InternalCounterImpl) value).getInternalCounterDelta();
                    break;
                default:
                    throw new AchillesException("Cannot encode value '" + value + "' for Cassandra for property '"
                            + propertyName + "' of type '" + type.name() + "'");
            }
        }
        return encoded;
    }

    public Object encode(Object entityValue) {
        return entityValue == null ? null : transcoder.encode(this, entityValue);
    }

    public Object encodeKey(Object entityValue) {
        return entityValue == null ? null : transcoder.encodeKey(this, entityValue);
    }

    public <T> List<Object> encode(List<T> entityValue) {
        return entityValue == null ? null : transcoder.encode(this, entityValue);
    }

    public Set<Object> encode(Set<?> entityValue) {
        return entityValue == null ? null : transcoder.encode(this, entityValue);
    }

    public Map<Object, Object> encode(Map<?, ?> entityValue) {
        return entityValue == null ? null : transcoder.encode(this, entityValue);
    }

    public List<Object> encodeToComponents(Object compoundKey, boolean onlyStaticColumns) {
        return compoundKey == null ? null : transcoder.encodeToComponents(this, compoundKey, onlyStaticColumns);
    }

    public List<Object> encodeToComponents(List<Object> components) {
        return components == null ? null : transcoder.encodeToComponents(this, components);
    }

    List<Object> encodePartitionComponents(List<Object> rawPartitionComponents) {
        return null;
    }

    List<Object> encodePartitionComponentsIN(List<Object> rawPartitionComponentsIN) {
        return null;
    }

    List<Object> encodeClusteringKeys(List<Object> rawClusteringKeys) {
        return null;
    }

    List<Object> encodeClusteringKeysIN(List<Object> rawClusteringKeysIN) {
        return null;
    }

    public String forceEncodeToJSON(Object object) {
        return transcoder.forceEncodeToJSON(object);
    }

    public Object forceDecodeFromJSON(String cassandraValue, Class<?> targetType) {
        return transcoder.forceDecodeFromJSON(cassandraValue, targetType);
    }

    public Object forceDecodeFromJSON(String cassandraValue) {
        return transcoder.forceDecodeFromJSON(cassandraValue, valueClass);
    }

    public Object getPrimaryKey(Object entity) {
        log.trace("Extract primary from entity {}", entity);
        if (type.isId()) {
            return invoker.getPrimaryKey(entity, this);
        } else {
            throw new IllegalStateException("Cannot get primary key on a non id field '" + propertyName + "'");
        }
    }

    public Object getPartitionKey(Object compoundKey) {
        log.trace("Extract partition key from primary compound key {}", compoundKey);
        if (type.isEmbeddedId()) {
            return invoker.getPartitionKey(compoundKey, this);
        } else {
            throw new IllegalStateException("Cannot get partition key on a non embedded id field '" + propertyName
                    + "'");
        }
    }

    public Object instantiate() {
        log.trace("Instantiate new entity of type{}", entityClassName);
        return invoker.instantiate(valueClass);
    }

    public Object getValueFromField(Object target) {
        return invoker.getValueFromField(target, field);
    }

    public Object invokeGetter(Object target) {
        return invoker.getValueFromField(target, getter);
    }

    public <T> List<T> getListValueFromField(Object target) {
        return invoker.getListValueFromField(target, field);
    }

    public <T> Set<T> getSetValueFromField(Object target) {
        return invoker.getSetValueFromField(target, field);
    }

    public <K, V> Map<K, V> getMapValueFromField(Object target) {
        return invoker.getMapValueFromField(target, field);
    }

    public void setValueToField(Object target, Object args) {
        invoker.setValueToField(target, field, args);
    }

    public Class<?> getValueClassForTableCreation() {
        if (timeUUID) {
            return InternalTimeUUID.class;
        } else {
            return valueClass;
        }
    }

    // //////// Getters & setters
    public PropertyType type() {
        return type;
    }

    public void setType(PropertyType propertyType) {
        this.type = propertyType;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> getKeyClass() {
        return (Class<T>) keyClass;
    }

    public void setKeyClass(Class<?> keyClass) {
        this.keyClass = keyClass;
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> getValueClass() {
        return (Class<T>) valueClass;
    }

    public void setValueClass(Class<?> valueClass) {
        this.valueClass = valueClass;
    }

    public Method getGetter() {
        return getter;
    }

    public void setGetter(Method getter) {
        this.getter = getter;
    }

    public Method getSetter() {
        return setter;
    }

    public void setSetter(Method setter) {
        this.setter = setter;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public EmbeddedIdProperties getEmbeddedIdProperties() {
        return embeddedIdProperties;
    }

    public void setEmbeddedIdProperties(EmbeddedIdProperties embeddedIdProperties) {
        this.embeddedIdProperties = embeddedIdProperties;
    }

    public CounterProperties getCounterProperties() {
        return counterProperties;
    }

    public void setCounterProperties(CounterProperties counterProperties) {
        this.counterProperties = counterProperties;
    }

    public void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        this.consistencyLevels = consistencyLevels;
    }

    public String getEntityClassName() {
        return entityClassName;
    }

    public void setEntityClassName(String entityClassName) {
        this.entityClassName = entityClassName;
    }

    public boolean isIndexed() {
        return this.indexProperties != null;
    }

    public IndexProperties getIndexProperties() {
        return indexProperties;
    }

    public void setIndexProperties(IndexProperties indexProperties) {
        this.indexProperties = indexProperties;
    }

    public DataTranscoder getTranscoder() {
        return transcoder;
    }

    public void setTranscoder(DataTranscoder transcoder) {
        this.transcoder = transcoder;
    }

    public ReflectionInvoker getInvoker() {
        return invoker;
    }

    public void setInvoker(ReflectionInvoker invoker) {
        this.invoker = invoker;
    }

    public boolean isTimeUUID() {
        return timeUUID;
    }

    public void setTimeUUID(boolean timeUUID) {
        this.timeUUID = timeUUID;
    }

    public void setEmptyCollectionAndMapIfNull(boolean emptyCollectionAndMapIfNull) {
        this.emptyCollectionAndMapIfNull = emptyCollectionAndMapIfNull;
    }

    public boolean isStaticColumn() {
        return staticColumn;
    }

    public void setStaticColumn(boolean staticColumn) {
        this.staticColumn = staticColumn;
    }

    public String getCQL3PropertyName() {
        return propertyName;
    }

    public Object nullValueForCollectionAndMap() {
        Object value = null;
        if (emptyCollectionAndMapIfNull) {
            switch (type) {
                case LIST:
                    value = new ArrayList<>();
                    break;
                case SET:
                    value = new HashSet<>();
                    break;
                case MAP:
                    value = new HashMap<>();
                    break;
                default:
                    break;
            }
        }
        return value;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass()).add("type", type).add("entityClassName", entityClassName)
                .add("propertyName", propertyName).add("keyClass", keyClass).add("valueClass", valueClass)
                .add("counterProperties", counterProperties).add("embeddedIdProperties", embeddedIdProperties)
                .add("consistencyLevels", consistencyLevels).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(entityClassName, propertyName, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PropertyMeta other = (PropertyMeta) obj;

        return Objects.equal(entityClassName, other.getEntityClassName())
                && Objects.equal(propertyName, other.getPropertyName()) && Objects.equal(type, other.type());
    }
}
