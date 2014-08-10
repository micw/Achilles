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
import static info.archinnov.achilles.internal.cql.TypeMapper.getRowMethod;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLDataType;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLType;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static java.lang.String.format;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.internal.table.ColumnMetaDataComparator;
import info.archinnov.achilles.internal.validation.Validator;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import info.archinnov.achilles.schemabuilder.Create;
import info.archinnov.achilles.type.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

public class EmbeddedIdProperties extends AbstractComponentProperties {

	private static final Logger log = LoggerFactory.getLogger(EmbeddedIdProperties.class);

    private final PartitionComponents partitionComponents;
	private final ClusteringComponents clusteringComponents;
    private final String entityClassName;
    private ColumnMetaDataComparator columnMetaDataComparator = new ColumnMetaDataComparator();
    private ReflectionInvoker invoker = new ReflectionInvoker();

    public EmbeddedIdProperties(PartitionComponents partitionComponents, ClusteringComponents clusteringComponents, List<PropertyMeta> keyMetas, String entityClassName) {
		super(keyMetas);
		this.partitionComponents = partitionComponents;
		this.clusteringComponents = clusteringComponents;
        this.entityClassName = entityClassName;
    }

    // Components Validation
	void validatePartitionComponents(String className,Object...partitionComponents) {
		this.partitionComponents.validatePartitionComponents(className, partitionComponents);
	}

    void validatePartitionComponentsIn(String className,Object...partitionComponents) {
        this.partitionComponents.validatePartitionComponentsIn(className, partitionComponents);
    }

	void validateClusteringComponents(String className, Object...clusteringComponents) {
		this.clusteringComponents.validateClusteringComponents(className, clusteringComponents);
	}

    void validateClusteringComponentsIn(String className, Object...clusteringComponents) {
        this.clusteringComponents.validateClusteringComponentsIn(className, clusteringComponents);
    }

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("partitionComponents", partitionComponents).add("clusteringComponents", clusteringComponents).toString();
	}

    // Transcoding
    public Object decodeFromComponents(Object newInstance, List<?> components) {
        log.trace("Decode from CQL components {}", components);
        Validator.validateTrue(components.size() == this.propertyMetas.size(), "There should be exactly '%s' Cassandra columns to decode into an '%s' instance", this.propertyMetas.size(), newInstance.getClass().getCanonicalName());
        for (int i = 0; i < propertyMetas.size(); i++) {
            final PropertyMeta componentMeta = propertyMetas.get(i);
            final Object decodedValue = componentMeta.decodeFromCassandra(components.get(i));
            componentMeta.setValueToField(newInstance,decodedValue);
        }
        return newInstance;
    }

    public List<Object> encodeToComponents(Object compoundKey, boolean onlyStaticColumns) {
        log.trace("Encode {} to CQL components with 'onlyStaticColumns' : {}", compoundKey, onlyStaticColumns);
        List<Object> encoded = new ArrayList<>();
        if (onlyStaticColumns) {
            for (PropertyMeta partitionKeyMeta : partitionComponents.propertyMetas) {
                encoded.add(partitionKeyMeta.getAndEncodeValueForCassandra(compoundKey));
            }
        } else {
            for (PropertyMeta partitionKeyMeta : propertyMetas) {
                encoded.add(partitionKeyMeta.getAndEncodeValueForCassandra(compoundKey));
            }
        }
        return encoded;
    }

    public List<Object> encodePartitionComponents(List<Object> rawPartitionComponents) {
        log.trace("Encode {} to CQL partition components", rawPartitionComponents);
        Validator.validateTrue(rawPartitionComponents.size() <= partitionComponents.propertyMetas.size(),"There should be no more than '%s' partition components to be encoded for class '%s'", rawPartitionComponents, entityClassName);
        return encodeElements(rawPartitionComponents, partitionComponents.propertyMetas);
    }

    public List<Object> encodePartitionComponentsIN(List<Object> rawPartitionComponentsIN) {
        log.trace("Encode {} to CQL partition components IN", rawPartitionComponentsIN);
        final List<PropertyMeta> partitionComponentMetas = partitionComponents.propertyMetas;
        final PropertyMeta lastPartitionComponentMeta = partitionComponentMetas.get(partitionComponentMetas.size() - 1);
        return encodeLastComponent(rawPartitionComponentsIN, lastPartitionComponentMeta);
    }

    public List<Object> encodeClusteringKeys(List<Object> rawClusteringKeys) {
        log.trace("Encode {} to CQL clustering keys", rawClusteringKeys);
        Validator.validateTrue(rawClusteringKeys.size() <= clusteringComponents.propertyMetas.size(),"There should be no more than '%s' clustering components to be encoded for class '%s'", rawClusteringKeys, entityClassName);
        return encodeElements(rawClusteringKeys, clusteringComponents.propertyMetas);
    }

    public List<Object> encodeClusteringKeysIN(List<Object> rawClusteringKeysIN) {
        log.trace("Encode {} to CQL clustering keys IN", rawClusteringKeysIN);
        final List<PropertyMeta> cluseringKeyMetas = clusteringComponents.propertyMetas;
        final PropertyMeta lastClusteringKeyMeta = cluseringKeyMetas.get(cluseringKeyMetas.size() - 1);
        return encodeLastComponent(rawClusteringKeysIN, lastClusteringKeyMeta);
    }

    private List<Object> encodeElements(List<Object> rawPartitionComponents, List<PropertyMeta> propertyMetas) {
        List<Object> encoded = new ArrayList<>();
        for (int i = 0; i < propertyMetas.size(); i++) {
            final PropertyMeta componentMeta = propertyMetas.get(i);
            encoded.add(componentMeta.encodeToCassandra(rawPartitionComponents.get(i)));
        }
        return encoded;
    }

    private List<Object> encodeLastComponent(List<Object> rawPartitionComponentsIN, PropertyMeta lastComponentMeta) {
        List<Object> encoded = new ArrayList<>();
        for (Object rawPartitionComponentIN : rawPartitionComponentsIN) {
            encoded.add(lastComponentMeta.encode(rawPartitionComponentIN));
        }
        return encoded;
    }

    // Utility
    public boolean isCompositePartitionKey() {
        return partitionComponents.isComposite();
    }

    public boolean isClustered() {
        return clusteringComponents.isClustered();
    }

    public String getOrderingComponent() {
        return clusteringComponents.getOrderingComponent();
    }

    public List<ClusteringOrder> getCluseringOrders() {
        return clusteringComponents.getClusteringOrders();
    }

    public List<String> getClusteringComponentNames() {
        return clusteringComponents.getCQL3ComponentNames();
    }

    public List<Class<?>> getClusteringComponentClasses() {
        return clusteringComponents.getComponentClasses();
    }

    public List<String> getPartitionComponentNames() {
        return partitionComponents.getCQL3ComponentNames();
    }

    public List<Class<?>> getPartitionComponentClasses() {
        return partitionComponents.getComponentClasses();
    }

    public List<Field> getPartitionComponentFields() {
        return partitionComponents.getComponentFields();
    }

    //CQL3 extraction
    List<Object> extractRawCompoundPrimaryComponentsFromRow(Row row) {
        final List<Class<?>> componentClasses = getComponentClasses();
        final List<String> cql3ComponentNames = getCQL3ComponentNames();
        List<Object> rawValues = new ArrayList<>(Collections.nCopies(cql3ComponentNames.size(), null));
        try {
            for (ColumnDefinitions.Definition column : row.getColumnDefinitions()) {
                String columnName = column.getName();
                int index = cql3ComponentNames.indexOf(columnName);
                Object rawValue;
                if (index >= 0) {
                    Class<?> componentClass = componentClasses.get(index);
                    rawValue = getRowMethod(componentClass).invoke(row, columnName);
                    rawValues.set(index, rawValue);
                }
            }
        } catch (Exception e) {
            throw new AchillesException(format("Cannot retrieve compound primary key for entity class '%s' from CQL Row", entityClassName), e);
        }
        return rawValues;
    }

    void validateExtractedCompoundPrimaryComponents(List<Object> rawComponents, Class<?> primaryKeyClass) {
        final List<String> cql3ComponentNames = getCQL3ComponentNames();
        for (int i = 0; i < cql3ComponentNames.size(); i++) {
            Validator.validateNotNull(rawComponents.get(i), "Error, the component '%s' from @EmbeddedId class '%s' cannot be found in Cassandra", cql3ComponentNames.get(i), primaryKeyClass);
        }
    }

    // CQL3 statement generation
    Delete.Where prepareWhereClauseForDelete(boolean onlyStaticColumns, Delete mainFrom) {
        Delete.Where where = null;
        List<String> componentNames;
        if (onlyStaticColumns) {
            componentNames = getPartitionComponentNames();
        } else {
            componentNames = getComponentNames();
        }

        int i = 0;
        for (String clusteredId : componentNames) {
            if (i ++== 0) {
                where = mainFrom.where(eq(clusteredId, bindMarker(clusteredId)));
            } else {
                where.and(eq(clusteredId, bindMarker(clusteredId)));
            }
        }
        return where;
    }

    Select.Where prepareWhereClauseForSelect(Optional<PropertyMeta> pmO, Select from) {
        Select.Where where = null;
        int i = 0;
        List<String> componentNames;
        if (pmO.isPresent() && pmO.get().isStaticColumn()) {
            componentNames = getPartitionComponentNames();
        } else {
            componentNames = getCQL3ComponentNames();
        }
        for (String partitionKey : componentNames) {
            if (i++ == 0) {
                where = from.where(eq(partitionKey, bindMarker(partitionKey)));
            } else {
                where.and(eq(partitionKey, bindMarker(partitionKey)));
            }
        }
        return where;
    }

    Insert prepareInsertPrimaryKey(Insert insert) {
        for (String component : getComponentNames()) {
            insert.value(component, bindMarker(component));
        }
        return insert;
    }

    Update.Where prepareCommonWhereClauseForUpdate(Update.Assignments assignments, boolean onlyStaticColumns, Update.Where where) {
        int i = 0;
        if (onlyStaticColumns) {
            for (String partitionKeys : getPartitionComponentNames()) {
                if (i++ == 0) {
                    where = assignments.where(eq(partitionKeys, bindMarker(partitionKeys)));
                } else {
                    where.and(eq(partitionKeys, bindMarker(partitionKeys)));
                }
            }
        } else {
            for (String clusteredId : getCQL3ComponentNames()) {
                if (i++ == 0) {
                    where = assignments.where(eq(clusteredId, bindMarker(clusteredId)));
                } else {
                    where.and(eq(clusteredId, bindMarker(clusteredId)));
                }
            }
        }
        return where;
    }

    Pair<Update.Where, Object[]> generateWhereClauseForUpdate(Object primaryKey, PropertyMeta pm, Update.Assignments update) {
        List<String> componentNames = getComponentNames();
        List<Object> encodedComponents = encodeToComponents(primaryKey, pm.isStaticColumn());
        Object[] boundValues = new Object[encodedComponents.size()];
        Update.Where where = null;
        for (int i = 0; i < encodedComponents.size(); i++) {
            String componentName = componentNames.get(i);
            Object componentValue = encodedComponents.get(i);
            if (i == 0) {
                where = update.where(eq(componentName, componentValue));
            } else {
                where.and(eq(componentName, componentValue));
            }
            boundValues[i] = componentValue;
        }
        return Pair.create(where, boundValues);
    }

    //Table validation
    void validatePrimaryKeyComponents(TableMetadata tableMetadata, boolean partitionKey) {
        log.debug("Validate existing primary key component from table {} against entity class {}",tableMetadata.getName(), entityClassName);

        if (partitionKey) {
            for (PropertyMeta partitionMeta : partitionComponents.propertyMetas) {
                validatePartitionComponent(tableMetadata, partitionMeta);
            }
        } else {
            for (PropertyMeta partitionMeta : clusteringComponents.propertyMetas) {
                validateClusteringComponent(tableMetadata, partitionMeta);
            }
        }
    }

    private void validatePartitionComponent(TableMetadata tableMetaData, PropertyMeta partitionMeta) {
        final String tableName = tableMetaData.getName();
        final String cql3PropertyName = partitionMeta.getCQL3PropertyName();
        final Class<?> columnJavaType = partitionMeta.getValueClassForTableCreationAndValidation();
        log.debug("Validate existing partition key component {} from table {} against type {}", cql3PropertyName, tableName, columnJavaType.getCanonicalName());

        // no ALTER's for partition components
        ColumnMetadata columnMetadata = tableMetaData.getColumn(cql3PropertyName);
        validateColumnType(tableName, cql3PropertyName, columnMetadata, columnJavaType);

        Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getPartitionKey(), columnMetadata),
                "Column '%s' of table '%s' should be a partition key component", cql3PropertyName, tableName);
    }


    private void validateClusteringComponent(TableMetadata tableMetaData, PropertyMeta clusteringMeta) {
        final String tableName = tableMetaData.getName();
        final String cql3PropertyName = clusteringMeta.getCQL3PropertyName();
        final Class<?> columnJavaType = clusteringMeta.getValueClassForTableCreationAndValidation();
        log.debug("Validate existing clustering column {} from table {} against type {}", cql3PropertyName,tableName, columnJavaType);

        // no ALTER's for clustering components
        ColumnMetadata columnMetadata = tableMetaData.getColumn(cql3PropertyName);
        validateColumnType(tableName, cql3PropertyName, columnMetadata, columnJavaType);
        Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getClusteringColumns(), columnMetadata),
                "Column '%s' of table '%s' should be a clustering key component", cql3PropertyName, tableName);
    }

    private void validateColumnType(String tableName, String columnName, ColumnMetadata columnMetadata, Class<?> columnJavaType) {
        DataType.Name expectedType = toCQLType(columnJavaType);
        DataType.Name realType = columnMetadata.getType().getName();
		/*
         * See JIRA
		 */
        if (realType == DataType.Name.CUSTOM) {
            realType = DataType.Name.BLOB;
        }
        Validator.validateTableTrue(expectedType == realType, "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName, realType, expectedType);
    }

    private boolean hasColumnMeta(Collection<ColumnMetadata> columnMetadatas, ColumnMetadata fqcnColumn) {
        boolean fqcnColumnMatches = false;
        for (ColumnMetadata columnMetadata : columnMetadatas) {
            fqcnColumnMatches = fqcnColumnMatches || columnMetaDataComparator.isEqual(fqcnColumn, columnMetadata);
        }
        return fqcnColumnMatches;
    }

    //Table creation
    void addPartitionKeys(Create createTable) {
        for (PropertyMeta partitionMeta: partitionComponents.propertyMetas) {
            String cql3PropertyName = partitionMeta.getCQL3PropertyName();
            Class<?> javaType = partitionMeta.getValueClassForTableCreationAndValidation();
            createTable.addPartitionKey(cql3PropertyName, toCQLDataType(javaType));
        }
    }

    void addClusteringKeys(Create createTable) {
        for (PropertyMeta clusteringMeta: clusteringComponents.propertyMetas) {
            String cql3PropertyName = clusteringMeta.getCQL3PropertyName();
            Class<?> javaType = clusteringMeta.getValueClassForTableCreationAndValidation();
            createTable.addClusteringKey(cql3PropertyName, toCQLDataType(javaType));
        }
    }

    //Slice query
    String getLastPartitionKeyName() {
        return Iterables.getLast(partitionComponents.getCQL3ComponentNames());
    }

    String getLastClusteringKeyName() {
        return Iterables.getLast(clusteringComponents.getCQL3ComponentNames());
    }

    int getPartitionKeysSize() {
        return partitionComponents.propertyMetas.size();
    }

    int getClusteringKeysSize() {
        return clusteringComponents.propertyMetas.size();
    }

    // Typed query
    public void validateTypedQuery(String queryString, Class<?> valueClass) {
        for (String component : getCQL3ComponentNames()) {
            Validator.validateTrue(queryString.contains(component),
                    "The typed query [%s] should contain the component column '%s' for embedded id type '%s'",
                    queryString, component, valueClass.getCanonicalName());
        }
    }

    public void copyPartitionComponentsToObject(Object newPrimaryKey, List<Object> partitionComponents) {
        List<Field> fields = getPartitionComponentFields();
        for (int i = 0; i < partitionComponents.size(); i++) {
            Field field = fields.get(i);
            Object component = partitionComponents.get(i);
            invoker.setValueToField(newPrimaryKey, field, component);
        }
    }
}
