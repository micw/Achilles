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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
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

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import info.archinnov.achilles.internal.metadata.transcoding.codec.*;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.json.DefaultJacksonMapper;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.persistence.operations.InternalCounterImpl;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

public class PropertyMeta {

    private static final Logger log = LoggerFactory.getLogger(PropertyMeta.class);
    private ObjectMapper defaultJacksonMapper = DefaultJacksonMapper.INSTANCE.get();

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
    private boolean emptyCollectionAndMapIfNull = false;
    private boolean staticColumn = false;
    private ReflectionInvoker invoker = new ReflectionInvoker();
    protected SimpleCodec simpleCodec;
    protected ListCodec listCodec;
    protected SetCodec setCodec;
    protected MapCodec mapCodec;

    // CQL3 Extraction
    public List<Object> extractRawCompoundPrimaryComponentsFromRow(Row row) {
        Validator.validateNotNull(embeddedIdProperties, "Cannot extract raw compound primary keys from CQL3 row because entity '%s' does not have a compound primary key",entityClassName);
        return embeddedIdProperties.extractRawCompoundPrimaryComponentsFromRow(row);
    }

    public void validateExtractedCompoundPrimaryComponents(List<Object> rawComponents, Class<?> primaryKeyClass) {
        Validator.validateNotNull(embeddedIdProperties, "Cannot validate raw compound primary keys from CQL3 row because entity '%s' does not have a compound primary key",entityClassName);
        embeddedIdProperties.validateExtractedCompoundPrimaryComponents(rawComponents, primaryKeyClass);
    }

    // CQL3 statements generation
    public RegularStatement prepareWhereClauseForDelete(boolean onlyStaticColumns, Delete mainFrom) {
        if (isEmbeddedId()) {
            return embeddedIdProperties.prepareWhereClauseForDelete(onlyStaticColumns, mainFrom);
        } else {
            return mainFrom.where(eq(getCQL3PropertyName(), bindMarker(getCQL3PropertyName())));
        }
    }

    public RegularStatement prepareWhereClauseForSelect(Optional<PropertyMeta> pmO, Select from) {
        if (isEmbeddedId()) {
            return embeddedIdProperties.prepareWhereClauseForSelect(pmO, from);
        } else {
            return from.where(eq(getCQL3PropertyName(), bindMarker(getCQL3PropertyName())));
        }
    }

    public Insert prepareInsertPrimaryKey(Insert insert) {
        if (isEmbeddedId()) {
            return embeddedIdProperties.prepareInsertPrimaryKey(insert);
        } else {
            return insert.value(getCQL3PropertyName(), bindMarker(getCQL3PropertyName()));
        }
    }

    public Update.Where prepareCommonWhereClauseForUpdate(Update.Assignments assignments, boolean onlyStaticColumns, Update.Where where) {
        Validator.validateNotNull(embeddedIdProperties, "Cannot prepare common WHERE clause for update because entity '%s' does not have a compound primary key",entityClassName);
        return embeddedIdProperties.prepareCommonWhereClauseForUpdate(assignments, onlyStaticColumns, where);
    }

    public Pair<Update.Where, Object[]> generateWhereClauseForUpdate(Object entity, PropertyMeta pm, Update.Assignments update) {
        Object primaryKey = getPrimaryKey(entity);
        if (isEmbeddedId()) {
            return embeddedIdProperties.generateWhereClauseForUpdate(primaryKey, pm, update);
        } else {
            Object id = encodeToCassandra(primaryKey);
            Update.Where where = update.where(eq(getCQL3PropertyName(), id));
            Object[] boundValues = new Object[] { id };
            return Pair.create(where, boundValues);
        }
    }

    public Select.Selection prepareSelectField(Select.Selection select) {
        if (isEmbeddedId()) {
            for (String component : embeddedIdProperties.getCQL3ComponentNames()) {
                select = select.column(component);
            }
            return select;
        } else {
            return select.column(getCQL3PropertyName());
        }
    }

    //Statement Caches
    public Set<String> extractClusteredFieldsIfNecessary() {
        if (isEmbeddedId()) {
            return new HashSet<>(embeddedIdProperties.getCQL3ComponentNames());
        } else {
            return Sets.newHashSet(getCQL3PropertyName());
        }
    }

    //Validation

    public List<String> getCQL3ComponentNames() {
        log.trace("Get CQL3 primary component names");
        Validator.validateNotNull(embeddedIdProperties, "Cannot retrieve CQL3 primary key component names because entity '%s' does not have a compound primary key",entityClassName);
        return embeddedIdProperties.getCQL3ComponentNames();
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
        if (embeddedIdProperties != null) {
            return  !embeddedIdProperties.getClusteringComponentClasses().isEmpty();
        }
        return false;
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
        return cassandraValue == null ? null : simpleCodec.decode(cassandraValue);
    }

    public List<Object> decode(List<?> cassandraValue) {
        return cassandraValue == null ? null : listCodec.decode(cassandraValue);
    }

    public Set<Object> decode(Set<?> cassandraValue) {
        return cassandraValue == null ? null : setCodec.decode(cassandraValue);
    }

    public Map<Object, Object> decode(Map<?, ?> cassandraValue) {
        return cassandraValue == null ? null : mapCodec.decode(cassandraValue);
    }

    public Object decodeFromComponents(List<?> components) {
        Validator.validateTrue(type == PropertyType.EMBEDDED_ID,"Cannot decode components '%s' for the property '%s' which is not a compound primary key", components, propertyName);
        return components == null ? null : embeddedIdProperties.decodeFromComponents(this.instantiate(), components);
    }

    public Object decodeFromCassandra(Object fromCassandra) {
        switch (type) {
            case SIMPLE:
            case ID:
                return simpleCodec.decode(fromCassandra);
            case LIST:
                return listCodec.decode((List)fromCassandra);
            case SET:
                return setCodec.decode((Set)fromCassandra);
            case MAP:
                return mapCodec.decode((Map)fromCassandra);
            default:
                throw new AchillesException(String.format("Cannot decode value '%s' from CQL3 for property '%s' of type '%s'",fromCassandra, propertyName, type.name()));
        }
    }

    public Object encodeToCassandra(Object fromJava) {
        switch (type) {
            case SIMPLE:
            case ID:
                return simpleCodec.encode(fromJava);
            case LIST:
                return listCodec.encode((List) fromJava);
            case SET:
                return setCodec.encode((Set) fromJava);
            case MAP:
                return mapCodec.encode((Map) fromJava);
            case COUNTER:
                return  ((InternalCounterImpl) fromJava).getInternalCounterDelta();
            default:
                throw new AchillesException(String.format("Cannot encode value '%s' to CQL3 for property '%s' of type '%s'",fromJava, propertyName, type.name()));
        }
    }

    public Object getAndEncodeValueForCassandra(Object entity) {
        Object value = getValueFromField(entity);
        if (value != null) {
            return encodeToCassandra(value);
        } else {
            return null;
        }
    }

    public Object encode(Object entityValue) {
        return entityValue == null ? null : simpleCodec.encode(entityValue);
    }

    public <T> List<Object> encode(List<T> entityValue) {
        return entityValue == null ? null : listCodec.encode(entityValue);
    }

    public Set<Object> encode(Set<?> entityValue) {
        return entityValue == null ? null : setCodec.encode(entityValue);
    }

    public Map<Object, Object> encode(Map<?, ?> entityValue) {
        return entityValue == null ? null : mapCodec.encode(entityValue);
    }

    public List<Object> encodeToComponents(Object compoundKey, boolean onlyStaticColumns) {
        Validator.validateTrue(type == PropertyType.EMBEDDED_ID,"Cannot encode object '%s' for the property '%s' which is not a compound primary key", compoundKey, propertyName);
        return compoundKey == null ? null : embeddedIdProperties.encodeToComponents(compoundKey, onlyStaticColumns);
    }


    List<Object> encodePartitionComponents(List<Object> rawPartitionComponents) {
        Validator.validateTrue(type == PropertyType.EMBEDDED_ID,"Cannot encode partition components '%s' for the property '%s' which is not a compound primary key", rawPartitionComponents, propertyName);
        return embeddedIdProperties.encodePartitionComponents(rawPartitionComponents);
    }

    List<Object> encodePartitionComponentsIN(List<Object> rawPartitionComponentsIN) {
        Validator.validateTrue(type == PropertyType.EMBEDDED_ID,"Cannot encode partition components '%s' for the property '%s' which is not a compound primary key", rawPartitionComponentsIN, propertyName);
        return embeddedIdProperties.encodePartitionComponentsIN(rawPartitionComponentsIN);
    }

    List<Object> encodeClusteringKeys(List<Object> rawClusteringKeys) {
        Validator.validateTrue(type == PropertyType.EMBEDDED_ID,"Cannot encode clustering components '%s' for the property '%s' which is not a compound primary key", rawClusteringKeys, propertyName);
        return embeddedIdProperties.encodeClusteringKeys(rawClusteringKeys);
    }

    List<Object> encodeClusteringKeysIN(List<Object> rawClusteringKeysIN) {
        Validator.validateTrue(type == PropertyType.EMBEDDED_ID,"Cannot encode clustering components '%s' for the property '%s' which is not a compound primary key", rawClusteringKeysIN, propertyName);
        return embeddedIdProperties.encodeClusteringKeysIN(rawClusteringKeysIN);
    }

    public String forceEncodeToJSON(Object object) {
        log.trace("Force encode {} to JSON", object);
        Validator.validateNotNull(object, "Cannot encode to JSON null primary key for class '%s'", entityClassName);
        if (object instanceof String) {
            return String.class.cast(object);
        } else {
            try {
                return this.defaultJacksonMapper.writeValueAsString(object);
            } catch (Exception e) {
                throw new AchillesException(String.format("Error while encoding primary key '%s' for class '%s'", object, entityClassName), e);
            }
        }
    }

    public Object getPrimaryKey(Object entity) {
        log.trace("Extract primary from entity {}", entity);
        if (type.isId()) {
            return invoker.getPrimaryKey(entity, this);
        } else {
            throw new IllegalStateException("Cannot get primary key on a non id field '" + propertyName + "'");
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

    public void setSimpleCodec(SimpleCodec simpleCodec) {
        this.simpleCodec = simpleCodec;
    }

    public void setListCodec(ListCodec listCodec) {
        this.listCodec = listCodec;
    }

    public void setSetCodec(SetCodec setCodec) {
        this.setCodec = setCodec;
    }
    public void setMapCodec(MapCodec mapCodec) {
        this.mapCodec = mapCodec;
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
