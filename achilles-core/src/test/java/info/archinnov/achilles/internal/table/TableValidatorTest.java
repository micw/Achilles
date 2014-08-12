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
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COUNTER;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.EMBEDDED_ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.MAP;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SET;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder.completeBean;
import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder.valueClass;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ColumnMetadata.IndexMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.IndexProperties;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.type.Counter;

@RunWith(MockitoJUnitRunner.class)
public class TableValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private TableValidator validator;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Cluster cluster;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private TableMetadata tableMetaData;

    @Mock
    private ColumnMetaDataComparator columnMetaDataComparator;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ColumnMetadata columnMetadata;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ColumnMetadata columnMetadataForField;

    @Mock
    private ConfigurationContext configContext;

    private String keyspaceName = "keyspace";

    private EntityMeta entityMeta;

    @Before
    public void setUp() {
        validator = new TableValidator();
        Whitebox.setInternalState(validator, "columnMetaDataComparator", columnMetaDataComparator);
        entityMeta = new EntityMeta();
        when(columnMetadata.getIndex()).thenReturn(null);
        when(columnMetadataForField.getIndex()).thenReturn(null);
    }

    @Test
    public void should_validate_id_for_entity() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
                .type(SIMPLE).build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setPropertyMetas(Maps.<String, PropertyMeta>newHashMap());
        entityMeta.setAllMetasExceptIdAndCounters(Arrays.asList(nameMeta));

        when(tableMetaData.getName()).thenReturn("table");
        when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
        when(columnMetadata.getType()).thenReturn(DataType.bigint());

        when(tableMetaData.getColumn("name")).thenReturn(columnMetadataForField);
        when(columnMetadataForField.getType()).thenReturn(DataType.text());

        validator.validateForEntity(entityMeta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_embedded_id_for_entity() throws Exception {
        PropertyMeta idMeta = valueClass(EmbeddedKey.class).type(EMBEDDED_ID).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
                .type(SIMPLE).build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptIdAndCounters(Arrays.asList(nameMeta));
        entityMeta.setPropertyMetas(Maps.<String, PropertyMeta>newHashMap());

        ColumnMetadata userColumn = mock(ColumnMetadata.class);
        ColumnMetadata nameColumn = mock(ColumnMetadata.class);

        when(tableMetaData.getName()).thenReturn("table");
        ColumnMetadata userIdMetadata = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn("userid")).thenReturn(userIdMetadata);
        when(userIdMetadata.getType()).thenReturn(DataType.bigint());
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(userColumn));
        when(columnMetaDataComparator.isEqual(userIdMetadata, userColumn)).thenReturn(true);

        ColumnMetadata nameMetadata = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn("name")).thenReturn(nameMetadata);
        when(nameMetadata.getType()).thenReturn(DataType.text());
        when(tableMetaData.getClusteringColumns()).thenReturn(Arrays.asList(nameColumn));
        when(columnMetaDataComparator.isEqual(nameMetadata, nameColumn)).thenReturn(true);

        when(tableMetaData.getColumn("string")).thenReturn(columnMetadataForField);
        when(columnMetadataForField.getType()).thenReturn(DataType.text());

        validator.validateForEntity(entityMeta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_embedded_id_with_time_uuid_for_entity() throws Exception {
        PropertyMeta idMeta = valueClass(EmbeddedKey.class).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
                .type(SIMPLE).build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptIdAndCounters(Arrays.asList(nameMeta));
        entityMeta.setPropertyMetas(Maps.<String, PropertyMeta>newHashMap());

        ColumnMetadata userColumn = mock(ColumnMetadata.class);
        ColumnMetadata nameColumn = mock(ColumnMetadata.class);

        when(tableMetaData.getName()).thenReturn("table");
        ColumnMetadata userIdMetadata = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn("userid")).thenReturn(userIdMetadata);
        when(userIdMetadata.getType()).thenReturn(DataType.bigint());
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(userColumn));
        when(columnMetaDataComparator.isEqual(userIdMetadata, userColumn)).thenReturn(true);

        ColumnMetadata nameMetadata = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn("date")).thenReturn(nameMetadata);
        when(nameMetadata.getType()).thenReturn(DataType.timeuuid());
        when(tableMetaData.getClusteringColumns()).thenReturn(Arrays.asList(nameColumn));
        when(columnMetaDataComparator.isEqual(nameMetadata, nameColumn)).thenReturn(true);

        when(tableMetaData.getColumn("name")).thenReturn(columnMetadataForField);
        when(columnMetadataForField.getType()).thenReturn(DataType.text());

        validator.validateForEntity(entityMeta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_simple_field_for_entity() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();

        PropertyMeta pm = completeBean(Void.class, String.class).field("name").type(SIMPLE)
                .build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptIdAndCounters(Arrays.asList(pm));
        entityMeta.setPropertyMetas(Maps.<String, PropertyMeta>newHashMap());

        when(tableMetaData.getName()).thenReturn("table");
        when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
        when(columnMetadata.getType()).thenReturn(DataType.bigint());
        when(columnMetadata.getIndex()).thenReturn(null);
        when(tableMetaData.getColumn("name")).thenReturn(columnMetadataForField);
        when(columnMetadataForField.getType()).thenReturn(DataType.text());
        when(columnMetadataForField.getIndex()).thenReturn(null);
        validator.validateForEntity(entityMeta, tableMetaData, configContext);

        pm = completeBean(Void.class, String.class).field("name").type(SIMPLE).build();
        entityMeta.setPropertyMetas(ImmutableMap.of("name", pm));
        validator.validateForEntity(entityMeta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_simple_indexed_field_for_entity() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();

        PropertyMeta pm = completeBean(Void.class, String.class).field("name").type(SIMPLE).build();
        pm.setIndexProperties(new IndexProperties("", ""));

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptIdAndCounters(Arrays.asList(pm));
        entityMeta.setPropertyMetas(Maps.<String, PropertyMeta>newHashMap());

        when(tableMetaData.getName()).thenReturn("table");
        when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
        when(columnMetadata.getType()).thenReturn(DataType.bigint());
        when(columnMetadata.getIndex()).thenReturn(null);
        when(tableMetaData.getColumn("name")).thenReturn(columnMetadataForField);
        when(columnMetadataForField.getType()).thenReturn(DataType.text());

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getName()).thenReturn("table(name)");
        when(indexMetadata.getIndexedColumn()).thenReturn(columnMetadataForField);

        when(columnMetadataForField.getIndex()).thenReturn(indexMetadata);
        validator.validateForEntity(entityMeta, tableMetaData, configContext);

    }

    @Test
    public void should_validate_list_field_for_entity() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();

        PropertyMeta pm = completeBean(Void.class, String.class).field("friends").type(LIST)
                .build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptIdAndCounters(Arrays.asList(pm));
        entityMeta.setPropertyMetas(Maps.<String, PropertyMeta>newHashMap());

        when(tableMetaData.getName()).thenReturn("table");
        when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
        when(columnMetadata.getType()).thenReturn(DataType.bigint());

        when(tableMetaData.getColumn("friends")).thenReturn(columnMetadataForField);
        when(columnMetadataForField.getType()).thenReturn(DataType.list(DataType.text()));

        validator.validateForEntity(entityMeta, tableMetaData, configContext);

        pm = completeBean(Void.class, String.class).field("friends").type(LIST).build();
        entityMeta.setPropertyMetas(ImmutableMap.of("friends", pm));
        validator.validateForEntity(entityMeta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_set_field_for_entity() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();

        PropertyMeta pm = completeBean(Void.class, String.class).field("followers").type(SET)
                .build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptIdAndCounters(Arrays.asList(pm));
        entityMeta.setPropertyMetas(Maps.<String, PropertyMeta>newHashMap());

        when(tableMetaData.getName()).thenReturn("table");
        when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
        when(columnMetadata.getType()).thenReturn(DataType.bigint());

        when(tableMetaData.getColumn("followers")).thenReturn(columnMetadataForField);
        when(columnMetadataForField.getType()).thenReturn(DataType.set(DataType.text()));

        validator.validateForEntity(entityMeta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_map_field_for_entity() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();

        PropertyMeta pm = completeBean(Integer.class, String.class).field("preferences")
                .type(MAP).build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptIdAndCounters(Arrays.asList(pm));
        entityMeta.setPropertyMetas(Maps.<String, PropertyMeta>newHashMap());

        when(tableMetaData.getName()).thenReturn("table");
        when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
        when(columnMetadata.getType()).thenReturn(DataType.bigint());

        when(tableMetaData.getColumn("preferences")).thenReturn(columnMetadataForField);
        when(columnMetadataForField.getType()).thenReturn(DataType.map(DataType.cint(), DataType.text()));

        validator.validateForEntity(entityMeta, tableMetaData, configContext);

        pm = completeBean(Integer.class, String.class).field("preferences").type(MAP)
                .build();
        entityMeta.setPropertyMetas(ImmutableMap.of("preferences", pm));
        validator.validateForEntity(entityMeta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_counter_fields() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();

        PropertyMeta counter = completeBean(Void.class, Counter.class).field("count")
                .type(COUNTER).build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptId(Arrays.asList(counter));
        entityMeta.setAllMetasExceptIdAndCounters(Arrays.<PropertyMeta>asList());
        entityMeta.setAllMetasExceptCounters(Arrays.<PropertyMeta>asList());
        entityMeta.setPropertyMetas(ImmutableMap.of("count", counter));

        when(tableMetaData.getName()).thenReturn("table");
        when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
        when(columnMetadata.getType()).thenReturn(DataType.bigint());

        when(columnMetadata.getIndex()).thenReturn(null);
        when(tableMetaData.getColumn("count")).thenReturn(columnMetadataForField);
        when(columnMetadataForField.getType()).thenReturn(DataType.counter());
        when(columnMetadataForField.getIndex()).thenReturn(null);

        //When
        validator.validateForEntity(entityMeta, tableMetaData, configContext);

        //Then

    }

    @Test
    public void should_validate_achilles_counter() throws Exception {
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        ColumnMetadata propertyColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(fqcnColumnMeta, fqcnColumn)).thenReturn(true);

        ColumnMetadata pkColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY)).thenReturn(pkColumnMeta);
        when(pkColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(pkColumnMeta, pkColumn)).thenReturn(true);

        ColumnMetadata propertyColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumnMeta);
        when(propertyColumnMeta.getType()).thenReturn(DataType.text());
        when(tableMetaData.getClusteringColumns()).thenReturn(Arrays.asList(propertyColumn));
        when(columnMetaDataComparator.isEqual(propertyColumnMeta, propertyColumn)).thenReturn(true);

        ColumnMetadata counterColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_VALUE)).thenReturn(counterColumnMeta);
        when(counterColumnMeta.getType()).thenReturn(DataType.counter());

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_table_not_found() throws Exception {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("Cannot find table '" + CQL_COUNTER_TABLE + "' from keyspace '" + keyspaceName + "'");

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_no_counter_fqcn_column() throws Exception {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Cannot find column '%s' from table '%s'", CQL_COUNTER_FQCN,
                CQL_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_fqcn_column_bad_type() {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.blob());

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Column '%s' of type '%s' should be of type '%s'", CQL_COUNTER_FQCN,
                fqcnColumnMeta.getType(), text()));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_not_matching_counter_fqcn_column() {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.text());

        // Then
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(String.format("Column '%s' of table '%s' should be a partition key component",
                CQL_COUNTER_FQCN, CQL_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_no_counter_pk_column() {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(fqcnColumnMeta, fqcnColumn)).thenReturn(true);

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Cannot find column '%s' from table '%s'", CQL_COUNTER_PRIMARY_KEY,
                CQL_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_pk_column_bad_type() {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(fqcnColumnMeta, fqcnColumn)).thenReturn(true);

        ColumnMetadata pkColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY)).thenReturn(pkColumnMeta);
        when(pkColumnMeta.getType()).thenReturn(DataType.blob());

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Column '%s' of type '%s' should be of type '%s'",
                CQL_COUNTER_PRIMARY_KEY, pkColumnMeta.getType(), text()));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_pk_column_not_matching() {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(fqcnColumnMeta, fqcnColumn)).thenReturn(true);

        ColumnMetadata pkColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY)).thenReturn(pkColumnMeta);
        when(pkColumnMeta.getType()).thenReturn(DataType.text());

        // Then
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(String.format("Column '%s' of table '%s' should be a partition key component",
                CQL_COUNTER_PRIMARY_KEY, CQL_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_no_counter_property_column() {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(fqcnColumnMeta, fqcnColumn)).thenReturn(true);

        ColumnMetadata pkColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY)).thenReturn(pkColumnMeta);
        when(pkColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(pkColumnMeta, pkColumn)).thenReturn(true);

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Cannot find column '%s' from table '%s'", CQL_COUNTER_PROPERTY_NAME,
                CQL_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_property_column_bad_type() {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(fqcnColumnMeta, fqcnColumn)).thenReturn(true);

        ColumnMetadata pkColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY)).thenReturn(pkColumnMeta);
        when(pkColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(pkColumnMeta, pkColumn)).thenReturn(true);

        ColumnMetadata propertyColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumnMeta);
        when(propertyColumnMeta.getType()).thenReturn(DataType.cfloat());

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Column '%s' of type '%s' should be of type '%s'",
                CQL_COUNTER_PROPERTY_NAME, propertyColumnMeta.getType(), text()));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_property_column_not_matching() {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        ColumnMetadata propertyColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(fqcnColumnMeta, fqcnColumn)).thenReturn(true);

        ColumnMetadata pkColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY)).thenReturn(pkColumnMeta);
        when(pkColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(pkColumnMeta, pkColumn)).thenReturn(true);

        ColumnMetadata propertyColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumnMeta);
        when(propertyColumnMeta.getType()).thenReturn(DataType.text());
        when(tableMetaData.getClusteringColumns()).thenReturn(Arrays.asList(propertyColumn));

        // Then
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(String.format("Column '%s' of table '%s' should be a clustering key component",
                CQL_COUNTER_PROPERTY_NAME, CQL_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_no_counter_value_column() {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        ColumnMetadata propertyColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(fqcnColumnMeta, fqcnColumn)).thenReturn(true);

        ColumnMetadata pkColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY)).thenReturn(pkColumnMeta);
        when(pkColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(pkColumnMeta, pkColumn)).thenReturn(true);

        ColumnMetadata propertyColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumnMeta);
        when(propertyColumnMeta.getType()).thenReturn(DataType.text());
        when(tableMetaData.getClusteringColumns()).thenReturn(Arrays.asList(propertyColumn));
        when(columnMetaDataComparator.isEqual(propertyColumnMeta, propertyColumn)).thenReturn(true);

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Cannot find column '%s' from table '%s'", CQL_COUNTER_VALUE,
                CQL_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_value_column_bad_type() {
        // Given
        tableMetaData = mock(TableMetadata.class);
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        ColumnMetadata propertyColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn, pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(fqcnColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(fqcnColumnMeta, fqcnColumn)).thenReturn(true);

        ColumnMetadata pkColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY)).thenReturn(pkColumnMeta);
        when(pkColumnMeta.getType()).thenReturn(DataType.text());
        when(columnMetaDataComparator.isEqual(pkColumnMeta, pkColumn)).thenReturn(true);

        ColumnMetadata propertyColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumnMeta);
        when(propertyColumnMeta.getType()).thenReturn(DataType.text());
        when(tableMetaData.getClusteringColumns()).thenReturn(Arrays.asList(propertyColumn));
        when(columnMetaDataComparator.isEqual(propertyColumnMeta, propertyColumn)).thenReturn(true);

        ColumnMetadata counterColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_VALUE)).thenReturn(counterColumnMeta);
        when(counterColumnMeta.getType()).thenReturn(DataType.cfloat());

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Column '%s' of type '%s' should be of type '%s'", CQL_COUNTER_VALUE,
                counterColumnMeta.getType(), counter()));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }
}
