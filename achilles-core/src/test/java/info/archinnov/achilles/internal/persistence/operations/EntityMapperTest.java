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
package info.archinnov.achilles.internal.persistence.operations;

import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState.MANAGED;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.EMBEDDED_ID;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ColumnDefinitionBuilder;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.internal.reflection.RowMethodInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

@RunWith(MockitoJUnitRunner.class)
public class EntityMapperTest {

    @InjectMocks
    private EntityMapper entityMapper;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private RowMethodInvoker cqlRowInvoker;

    @Mock
    private Row row;

    @Mock
    private ColumnDefinitions columnDefs;

    @Mock
    private EntityMeta entityMeta;

    @Captor
    private ArgumentCaptor<InternalCounterImpl> counterCaptor;

    private Definition def1;
    private Definition def2;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    @Test
    public void should_set_non_counter_properties_to_entity() throws Exception {
        PropertyMeta pm = mock(PropertyMeta.class);
        when(pm.structure().isEmbeddedId()).thenReturn(false);
        when(pm.getPropertyName()).thenReturn("name");
        when(entityMeta.getAllMetasExceptCounters()).thenReturn(asList(pm));

        when(row.isNull("name")).thenReturn(false);
        when(cqlRowInvoker.invokeOnRowForFields(row, pm)).thenReturn("value");

        entityMapper.setNonCounterPropertiesToEntity(row, entityMeta, entity);

        verify(pm).setValueToField(entity, "value");
    }

    @Test
    public void should_set_value_to_clustered_counter_entity() throws Exception {
        //Given
        Long counterValue = 10L;
        PropertyMeta counterMeta = mock(PropertyMeta.class);
        when(counterMeta.getPropertyName()).thenReturn("counter");
        when(entityMeta.getAllCounterMetas()).thenReturn(asList(counterMeta));
        when(cqlRowInvoker.invokeOnRowForType(row, Long.class, "counter")).thenReturn(counterValue);

        //When
        entityMapper.setValuesToClusteredCounterEntity(row, entityMeta, entity);

        //Then
        verify(counterMeta).setValueToField(eq(entity), counterCaptor.capture());

        assertThat(counterCaptor.getValue().get()).isEqualTo(counterValue);
    }

    @Test
    public void should_set_null_to_entity_when_no_value_from_row() throws Exception {
        PropertyMeta pm = mock(PropertyMeta.class);
        when(pm.structure().isEmbeddedId()).thenReturn(false);
        when(pm.getPropertyName()).thenReturn("name");

        when(row.isNull("name")).thenReturn(true);

        entityMapper.setNonCounterPropertiesToEntity(row, entityMeta, entity);

        verify(pm, never()).setValueToField(eq(entity), any());
        verifyZeroInteractions(cqlRowInvoker);
    }

    @Test
    public void should_do_nothing_when_null_row() throws Exception {
        PropertyMeta pm = mock(PropertyMeta.class);

        entityMapper.setPropertyToEntity(null, entityMeta, pm, entity);

        verifyZeroInteractions(cqlRowInvoker);
    }

    @Test
    public void should_set_compound_key_to_entity() throws Exception {
        PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
                .type(EMBEDDED_ID).invoker(invoker).build();

        EmbeddedKey embeddedKey = new EmbeddedKey();
        when(cqlRowInvoker.extractCompoundPrimaryKeyFromRow(row, entityMeta, pm, MANAGED)).thenReturn(embeddedKey);

        entityMapper.setPropertyToEntity(row, entityMeta, pm, entity);

        verify(invoker).setValueToField(entity, pm.getField(), embeddedKey);
    }

    @Test
    public void should_map_row_to_entity() throws Exception {
        Long id = RandomUtils.nextLong();
        PropertyMeta idMeta = mock(PropertyMeta.class);
        PropertyMeta valueMeta = mock(PropertyMeta.class);

        when(idMeta.structure().isEmbeddedId()).thenReturn(false);

        Map<String, PropertyMeta> propertiesMap = ImmutableMap.of("id", idMeta, "value", valueMeta);

        def1 = ColumnDefinitionBuilder.buildColumnDef("keyspace", "table", "id", DataType.bigint());
        def2 = ColumnDefinitionBuilder.buildColumnDef("keyspace", "table", "value", DataType.text());

        when(row.getColumnDefinitions()).thenReturn(columnDefs);
        when(columnDefs.iterator()).thenReturn(asList(def1, def2).iterator());

        when(entityMeta.getIdMeta()).thenReturn(idMeta);
        when(entityMeta.instanciate()).thenReturn(entity);
        when(cqlRowInvoker.invokeOnRowForFields(row, idMeta)).thenReturn(id);
        when(cqlRowInvoker.invokeOnRowForFields(row, valueMeta)).thenReturn("value");
        when(entityMeta.instanciate()).thenReturn(entity);

        CompleteBean actual = entityMapper.mapRowToEntityWithPrimaryKey(entityMeta, row, propertiesMap, MANAGED);

        assertThat(actual).isSameAs(entity);
        verify(idMeta).setValueToField(entity, id);
        verify(valueMeta).setValueToField(entity, "value");
    }

    @Test
    public void should_map_row_to_entity_with_primary_key() throws Exception {
        ClusteredEntity entity = new ClusteredEntity();
        EmbeddedKey embeddedKey = new EmbeddedKey();
        PropertyMeta idMeta = mock(PropertyMeta.class);

        when(idMeta.structure().isEmbeddedId()).thenReturn(true);

        Map<String, PropertyMeta> propertiesMap = new HashMap<>();

        when(row.getColumnDefinitions()).thenReturn(columnDefs);
        when(columnDefs.iterator()).thenReturn(Arrays.<Definition>asList().iterator());
        when(entityMeta.instanciate()).thenReturn(entity);
        when(entityMeta.getIdMeta()).thenReturn(idMeta);
        when(cqlRowInvoker.extractCompoundPrimaryKeyFromRow(row, entityMeta, idMeta, MANAGED)).thenReturn(embeddedKey);

        ClusteredEntity actual = entityMapper.mapRowToEntityWithPrimaryKey(entityMeta, row, propertiesMap, MANAGED);

        assertThat(actual).isSameAs(entity);
        verify(idMeta).setValueToField(entity, embeddedKey);
    }

    @Test
    public void should_not_map_row_to_entity_with_primary_key_when_entity_null() {
        ClusteredEntity actual = entityMapper.mapRowToEntityWithPrimaryKey(entityMeta, row, null, MANAGED);

        assertThat(actual).isNull();
    }

    @Test
    public void should_return_null_when_no_column_found() throws Exception {
        when(row.getColumnDefinitions()).thenReturn(null);
        when(entityMeta.instanciate()).thenReturn(entity);

        CompleteBean actual = entityMapper.mapRowToEntityWithPrimaryKey(entityMeta, row, null, MANAGED);
        assertThat(actual).isNull();
    }

}
