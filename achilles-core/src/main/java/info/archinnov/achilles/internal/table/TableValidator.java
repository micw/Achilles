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
package info.archinnov.achilles.internal.table;

import static com.datastax.driver.core.DataType.counter;
import static com.datastax.driver.core.DataType.text;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLType;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.Counter;

public class TableValidator {

    private static final Logger log = LoggerFactory.getLogger(TableValidator.class);

    private ColumnMetaDataComparator columnMetaDataComparator = new ColumnMetaDataComparator();

    public void validateForEntity(EntityMeta entityMeta, TableMetadata tableMetadata, ConfigurationContext configContext) {
        log.debug("Validate existing table {} for {}", tableMetadata.getName(), entityMeta);

        validateTable(entityMeta, tableMetadata, configContext);

    }

    private void validateTable(EntityMeta entityMeta, TableMetadata tableMetadata, ConfigurationContext configContext) {
        PropertyMeta idMeta = entityMeta.getIdMeta();
        if (entityMeta.isEmbeddedId()) {
            validatePrimaryKeyComponents(tableMetadata, idMeta, true);
            validatePrimaryKeyComponents(tableMetadata, idMeta, false);
        } else {
            validateColumn(tableMetadata, entityMeta, idMeta, configContext);
        }

        for (PropertyMeta pm : entityMeta.getAllMetasExceptIdAndCounters()) {
            switch (pm.type()) {
                case SIMPLE:
                    validateColumn(tableMetadata, entityMeta, pm, configContext);
                    break;
                case LIST:
                case SET:
                case MAP:
                    validateCollectionAndMapColumn(tableMetadata, pm, entityMeta.isSchemaUpdateEnabled());
                    break;
                default:
                    break;
            }
        }
        for (PropertyMeta counterMeta : entityMeta.getAllCounterMetas()) {
            validateCounterColumnForClusteredCounters(tableMetadata, counterMeta, entityMeta.isSchemaUpdateEnabled());
        }
    }

    public void validateAchillesCounter(KeyspaceMetadata keyspaceMetaData, String keyspaceName) {
        log.debug("Validate existing Achilles Counter table");
        Name textTypeName = text().getName();
        Name counterTypeName = counter().getName();

        TableMetadata tableMetaData = keyspaceMetaData.getTable(CQL_COUNTER_TABLE);
        Validator.validateTableTrue(tableMetaData != null, "Cannot find table '%s' from keyspace '%s'",
                CQL_COUNTER_TABLE, keyspaceName);

        ColumnMetadata fqcnColumn = tableMetaData.getColumn(CQL_COUNTER_FQCN);
        Validator.validateTableTrue(fqcnColumn != null, "Cannot find column '%s' from table '%s'", CQL_COUNTER_FQCN,
                CQL_COUNTER_TABLE);
        Validator.validateTableTrue(fqcnColumn.getType().getName() == textTypeName,
                "Column '%s' of type '%s' should be of type '%s'", CQL_COUNTER_FQCN, fqcnColumn.getType().getName(),
                textTypeName);
        Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getPartitionKey(), fqcnColumn),
                "Column '%s' of table '%s' should be a partition key component", CQL_COUNTER_FQCN, CQL_COUNTER_TABLE);

        ColumnMetadata pkColumn = tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY);
        Validator.validateTableTrue(pkColumn != null, "Cannot find column '%s' from table '%s'",
                CQL_COUNTER_PRIMARY_KEY, CQL_COUNTER_TABLE);
        Validator.validateTableTrue(pkColumn.getType().getName() == textTypeName,
                "Column '%s' of type '%s' should be of type '%s'", CQL_COUNTER_PRIMARY_KEY, pkColumn.getType()
                        .getName(), textTypeName);
        Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getPartitionKey(), pkColumn),
                "Column '%s' of table '%s' should be a partition key component", CQL_COUNTER_PRIMARY_KEY,
                CQL_COUNTER_TABLE);

        ColumnMetadata propertyNameColumn = tableMetaData.getColumn(CQL_COUNTER_PROPERTY_NAME);
        Validator.validateTableTrue(propertyNameColumn != null, "Cannot find column '%s' from table '%s'",
                CQL_COUNTER_PROPERTY_NAME, CQL_COUNTER_TABLE);
        Validator.validateTableTrue(propertyNameColumn.getType().getName() == textTypeName,
                "Column '%s' of type '%s' should be of type '%s'", CQL_COUNTER_PROPERTY_NAME, propertyNameColumn
                        .getType().getName(), textTypeName);
        Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getClusteringColumns(), propertyNameColumn),
                "Column '%s' of table '%s' should be a clustering key component", CQL_COUNTER_PROPERTY_NAME,
                CQL_COUNTER_TABLE);

        ColumnMetadata counterValueColumn = tableMetaData.getColumn(CQL_COUNTER_VALUE);
        Validator.validateTableTrue(counterValueColumn != null, "Cannot find column '%s' from table '%s'",
                CQL_COUNTER_VALUE, CQL_COUNTER_TABLE);
        Validator.validateTableTrue(counterValueColumn.getType().getName() == counterTypeName,
                "Column '%s' of type '%s' should be of type '%s'", CQL_COUNTER_VALUE, counterValueColumn.getType()
                        .getName(), counterTypeName);
    }

    private void validateCounterColumnForClusteredCounters(TableMetadata tableMetaData, PropertyMeta propertyMeta, boolean schemaUpdateEnabled) {
        String columnName = propertyMeta.getPropertyName().toLowerCase();

        log.debug("Validate existing column {} from table {} against type {}", columnName, tableMetaData.getName(), Counter.class);

        String tableName = tableMetaData.getName();
        ColumnMetadata columnMetadata = tableMetaData.getColumn(columnName);

        if (schemaUpdateEnabled && columnMetadata == null) {
            // will be created in updater
            return;
        } else {
            Validator.validateTableTrue(columnMetadata != null, "Cannot find column '%s' in the table '%s'", columnName, tableName);
        }

        Name realType = columnMetadata.getType().getName();

        Validator.validateTableTrue(realType == Name.COUNTER, "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName,
                realType, Name.COUNTER);
    }

    private void validateCollectionAndMapColumn(TableMetadata tableMetadata, PropertyMeta pm, boolean schemaUpdateEnabled) {

        log.debug("Validate existing collection/map column {} from table {}");

        String columnName = pm.getPropertyName().toLowerCase();
        String tableName = tableMetadata.getName();
        ColumnMetadata columnMetadata = tableMetadata.getColumn(columnName);

        if (schemaUpdateEnabled && columnMetadata == null) {
            // will be created in updater
            return;
        } else {
            Validator.validateTableTrue(columnMetadata != null, "Cannot find column '%s' in the table '%s'", columnName, tableName);
        }
        Name realType = columnMetadata.getType().getName();
        Name expectedValueType = toCQLType(pm.getValueClassForTableCreationAndValidation());

        switch (pm.type()) {
            case LIST:
                Validator.validateTableTrue(realType == Name.LIST,
                        "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName,
                        realType, Name.LIST);
                Name realListValueType = columnMetadata.getType().getTypeArguments().get(0).getName();
                Validator.validateTableTrue(realListValueType == expectedValueType,
                        "Column '%s' of table '%s' of type 'List<%s>' should be of type 'List<%s>' indeed", columnName,
                        tableName, realListValueType, expectedValueType);

                break;
            case SET:
                Validator.validateTableTrue(realType == Name.SET,
                        "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName,
                        realType, Name.SET);
                Name realSetValueType = columnMetadata.getType().getTypeArguments().get(0).getName();

                Validator.validateTableTrue(realSetValueType == expectedValueType,
                        "Column '%s' of table '%s' of type 'Set<%s>' should be of type 'Set<%s>' indeed", columnName,
                        tableName, realSetValueType, expectedValueType);
                break;
            case MAP:
                Validator.validateTableTrue(realType == Name.MAP,
                        "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName,
                        realType, Name.MAP);

                Name expectedMapKeyType = toCQLType(pm.getKeyClass());
                Name realMapKeyType = columnMetadata.getType().getTypeArguments().get(0).getName();
                Name realMapValueType = columnMetadata.getType().getTypeArguments().get(1).getName();
                Validator.validateTableTrue(realMapKeyType == expectedMapKeyType,
                        "Column %s' of table '%s' of type 'Map<%s,?>' should be of type 'Map<%s,?>' indeed", columnName,
                        tableName, realMapKeyType, expectedMapKeyType);

                Validator.validateTableTrue(realMapValueType == expectedValueType,
                        "Column '%s' of table '%s' of type 'Map<?,%s>' should be of type 'Map<?,%s>' indeed", columnName,
                        tableName, realMapValueType, expectedValueType);
                break;
            default:
                break;
        }
    }

    private void validatePrimaryKeyComponents(TableMetadata tableMetadata, PropertyMeta idMeta, boolean partitionKey) {
        log.debug("Validate existing primary key component from table {} against Achilles meta data {}",tableMetadata.getName(), idMeta);
        idMeta.validatePrimaryKeyComponents(tableMetadata, partitionKey);
    }

    private boolean hasColumnMeta(Collection<ColumnMetadata> columnMetadatas, ColumnMetadata fqcnColumn) {
        boolean fqcnColumnMatches = false;
        for (ColumnMetadata columnMetadata : columnMetadatas) {
            fqcnColumnMatches = fqcnColumnMatches || columnMetaDataComparator.isEqual(fqcnColumn, columnMetadata);
        }
        return fqcnColumnMatches;
    }

    private void validateColumn(TableMetadata tableMetaData, EntityMeta entityMeta, PropertyMeta propertyMeta,  ConfigurationContext configContext) {

        final String columnName = propertyMeta.getCQL3PropertyName();
        final Class<?> columnJavaType = propertyMeta.getValueClassForTableCreationAndValidation();
        final boolean schemaUpdateEnabled = entityMeta.isSchemaUpdateEnabled();
        String tableName = tableMetaData.getName();

        log.debug("Validate existing column {} from table {} against type {}", columnName, tableName, columnJavaType);

        ColumnMetadata columnMetadata = tableMetaData.getColumn(columnName);


        if (schemaUpdateEnabled && columnMetadata == null) {
            // will be created in updater
            return;
        } else {
            Validator.validateTableTrue(columnMetadata != null, "Cannot find column '%s' in the table '%s'", columnName, tableName);
        }

        validateColumnType(tableName, columnName, columnMetadata, columnJavaType);


        if (!configContext.isRelaxIndexValidation()) {
            boolean columnIsIndexed = columnMetadata.getIndex() != null;
            Validator.validateTableFalse((columnIsIndexed ^ propertyMeta.isIndexed()),"Column '%s' in the table '%s' is indexed (or not) whereas metadata indicates it is (or not)",columnName, tableName);
        }
    }

    private void validateColumnType(String tableName, String columnName, ColumnMetadata columnMetadata, Class<?> columnJavaType) {
        Name expectedType = toCQLType(columnJavaType);
        Name realType = columnMetadata.getType().getName();
		/*
         * See JIRA
		 */
        if (realType == Name.CUSTOM) {
            realType = Name.BLOB;
        }
        Validator.validateTableTrue(expectedType == realType, "Column '%s' of table '%s' of type '%s' should be of type '%s' indeed", columnName, tableName, realType, expectedType);
    }
}
