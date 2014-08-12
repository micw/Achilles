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
package info.archinnov.achilles.internal.statement;

import static com.datastax.driver.core.querybuilder.Update.Conditions;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.ID;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST_AT_INDEX;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.SET_TO_LIST_AT_INDEX;
import static info.archinnov.achilles.test.builders.CompleteBeanTestBuilder.builder;
import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder.completeBean;
import static info.archinnov.achilles.type.Options.CASCondition;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Where;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.Pair;

@RunWith(MockitoJUnitRunner.class)
public class StatementGeneratorTest {

    @InjectMocks
    private StatementGenerator generator;

    @Mock
    private DaoContext daoContext;

    @Mock
    private PersistenceContext.StateHolderFacade context;

    @Mock
    private RegularStatementWrapper statementWrapper;

    @Mock
    private DirtyCheckChangeSet dirtyCheckChangeSet;

    @Captor
    private ArgumentCaptor<Select> selectCaptor;

    @Captor
    private ArgumentCaptor<Conditions> conditionsCaptor;

    @Captor
    private ArgumentCaptor<Delete> deleteCaptor;

    private ReflectionInvoker invoker = new ReflectionInvoker();

    @Test
    public void should_generate_set_element_at_index_to_list_with_cas_conditions() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[] { "whatever" };
        CompleteBean entity = builder().id(id).buid();

        when(context.getEntity()).thenReturn(entity);
        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getTtl()).thenReturn(Optional.fromNullable(10));
        when(context.getTimestamp()).thenReturn(Optional.fromNullable(100L));

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(SET_TO_LIST_AT_INDEX);
        when(dirtyCheckChangeSet.generateUpdateForSetAtIndexElement(conditionsCaptor.capture())).thenReturn(Pair.create(update(), boundValues));

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(context, dirtyCheckChangeSet);

        //Then
        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE table USING TTL 10 AND TIMESTAMP 100;");
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id=" + id + ";");
        assertThat(pair.right[0]).isEqualTo(10);
        assertThat(pair.right[1]).isEqualTo(100L);
    }

    @Test
    public void should_generate_remove_element_at_index_to_list_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[] { "whatever" };

        EntityMeta meta = mock(EntityMeta.class);
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.encodeBoundValuesForTypedQueries(boundValues)).thenReturn(boundValues);

        CompleteBean entity = builder().id(id).buid();

        when(context.getTtl()).thenReturn(Optional.<Integer>absent());
        when(context.getTimestamp()).thenReturn(Optional.<Long>absent());
        when(context.getEntity()).thenReturn(entity);
        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getCasConditions()).thenReturn(asList(new CASCondition("name", "John")));
        when(dirtyCheckChangeSet.getChangeType()).thenReturn(REMOVE_FROM_LIST_AT_INDEX);
        when(dirtyCheckChangeSet.generateUpdateForRemovedAtIndexElement(any(Conditions.class))).thenReturn(Pair.create(update(), boundValues));

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(context, dirtyCheckChangeSet);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id=" + id + ";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    private Update.Assignments update() {
        return QueryBuilder.update("table").with();
    }


}
