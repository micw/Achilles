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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.internal.metadata.transcoding.codec.*;
import info.archinnov.achilles.json.DefaultJacksonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

public class PropertyMeta {

    private static final Logger log = LoggerFactory.getLogger(PropertyMeta.class);

    private ReflectionInvoker invoker = new ReflectionInvoker();
    public static final Predicate<PropertyMeta> STATIC_COLUMN_FILTER = new Predicate<PropertyMeta>() {
        @Override
        public boolean apply(PropertyMeta pm) {
            return pm.isStaticColumn();
        }
    };

    public static final Predicate<PropertyMeta> COUNTER_COLUMN_FILTER = new Predicate<PropertyMeta>() {
        @Override
        public boolean apply(PropertyMeta pm) {
            return pm.structure().isCounter();
        }
    };

    ObjectMapper defaultJacksonMapper = DefaultJacksonMapper.INSTANCE.get();

    PropertyType type;
    String propertyName;
    String entityClassName;
    Class<?> keyClass;
    Class<?> valueClass;
    Method getter;
    Method setter;
    Field field;
    CounterProperties counterProperties;
    EmbeddedIdProperties embeddedIdProperties;
    IndexProperties indexProperties;
    Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
    boolean timeUUID = false;
    boolean emptyCollectionAndMapIfNull = false;
    boolean staticColumn = false;
    SimpleCodec simpleCodec;
    ListCodec listCodec;
    SetCodec setCodec;
    MapCodec mapCodec;

    // CQL3 Extraction
    public PropertyMetaRowExtractor forRowExtraction() {
        return new PropertyMetaRowExtractor(this);
    }

    // CQL3 statements generation
    public PropertyMetaStatementGenerator forStatementGeneration() {
        return new PropertyMetaStatementGenerator(this);
    }

    //Statement Caches
    public PropertyMetaCacheSupport forCache() {
        return new PropertyMetaCacheSupport(this);
    }

    //Table Validation
    public PropertyMetaTableValidator forTableValidation() {
        return new PropertyMetaTableValidator(this);
    }

    //Table creation
    public PropertyMetaTableCreator forTableCreation() {
        return new PropertyMetaTableCreator(this);
    }

    //Slice query
    PropertyMetaSliceQuerySupport forSliceQuery() {
        return new PropertyMetaSliceQuerySupport(this);
    }

    public PropertyMetaSliceQueryContext forSliceQueryContext() {
        return new PropertyMetaSliceQueryContext(this);
    }

    //Typed query
    public PropertyMetaTypedQuery forTypedQuery() {
        return new PropertyMetaTypedQuery(this);
    }

    public PropertyMetaTranscoder forTranscoding() {
        return new PropertyMetaTranscoder(this);
    }

    public PropertyMetaStructure structure() {
        return new PropertyMetaStructure(this);
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

    public Object forceLoad(Object target) {
        return invoker.getValueFromField(target, getter);
    }

    public Object getValueFromField(Object target) {
        return invoker.getValueFromField(target, field);
    }

    public void setValueToField(Object target, Object args) {
        invoker.setValueToField(target, field, args);
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

    void setEmbeddedIdProperties(EmbeddedIdProperties embeddedIdProperties) {
        this.embeddedIdProperties = embeddedIdProperties;
    }

    public void setIdMetaForCounterProperties(PropertyMeta idMeta) {
        counterProperties.setIdMeta(idMeta);
    }

    void setCounterProperties(CounterProperties counterProperties) {
        this.counterProperties = counterProperties;
    }

    public void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        this.consistencyLevels = consistencyLevels;
    }

    public String getEntityClassName() {
        return entityClassName;
    }

    void setEntityClassName(String entityClassName) {
        this.entityClassName = entityClassName;
    }

    public void setIndexProperties(IndexProperties indexProperties) {
        this.indexProperties = indexProperties;
    }

    void setInvoker(ReflectionInvoker invoker) {
        this.invoker = invoker;
    }

    void setTimeUUID(boolean timeUUID) {
        this.timeUUID = timeUUID;
    }

    void setEmptyCollectionAndMapIfNull(boolean emptyCollectionAndMapIfNull) {
        this.emptyCollectionAndMapIfNull = emptyCollectionAndMapIfNull;
    }

    public boolean isStaticColumn() {
        return staticColumn;
    }

    void setStaticColumn(boolean staticColumn) {
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
