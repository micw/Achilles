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
package info.archinnov.achilles.internal.reflection;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;

@RunWith(MockitoJUnitRunner.class)
public class RowMethodInvokerTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private RowMethodInvoker invoker = new RowMethodInvoker();

    @Mock
    private Row row;

    @Mock
    private ColumnDefinitions columnDefs;

    @Mock
    private Definition definition;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta pm;

    private List<String> compNames;

    private List<Class<?>> compClasses;

    @Before
    public void setUp() {

        compNames = new ArrayList<>();
        compClasses = new ArrayList<>();

        when(pm.getPropertyName()).thenReturn("property");
        when(pm.<Integer>getKeyClass()).thenReturn(Integer.class);
        when(pm.<String>getValueClass()).thenReturn(String.class);
        when(row.isNull("property")).thenReturn(false);
//        when(pm.getComponentNames()).thenReturn(compNames);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void should_get_list_value_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.LIST);
        List<String> list = Arrays.asList("value");
        when(row.getList("property", String.class)).thenReturn(list);
        when(pm.forTranscoding().decode(list)).thenReturn((List) list);

        Object actual = invoker.invokeOnRowForFields(row, pm);

        assertThat((List) actual).containsAll(list);
    }

    @Test
    public void should_get_empty_list_value_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.LIST);
        when(row.isNull("property")).thenReturn(true);
        final ArrayList<Object> emptyList = new ArrayList<>();
        when(pm.nullValueForCollectionAndMap()).thenReturn(emptyList);

        Object actual = invoker.invokeOnRowForFields(row, pm);

        assertThat(actual).isSameAs(emptyList);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void should_get_set_value_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.SET);

        Set<String> set = Sets.newHashSet("value");
        when(row.getSet("property", String.class)).thenReturn(set);
        when(pm.forTranscoding().decode(set)).thenReturn((Set) set);

        Object actual = invoker.invokeOnRowForFields(row, pm);

        assertThat((Set) actual).containsAll(set);
    }

    @Test
    public void should_get_empty_set_value_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.SET);
        when(row.isNull("property")).thenReturn(true);
        final HashSet<Object> emptySet = new HashSet<>();
        when(pm.nullValueForCollectionAndMap()).thenReturn(emptySet);

        Object actual = invoker.invokeOnRowForFields(row, pm);

        assertThat(actual).isSameAs(emptySet);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void should_get_map_value_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.MAP);
        Map<Integer, String> map = ImmutableMap.of(11, "value");
        when(row.getMap("property", Integer.class, String.class)).thenReturn(map);
        when(pm.forTranscoding().decode(map)).thenReturn((Map) map);

        Object actual = invoker.invokeOnRowForFields(row, pm);

        assertThat((Map) actual).containsKey(11);
        assertThat((Map) actual).containsValue("value");
    }

    @Test
    public void should_get_empty_map_value_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.MAP);
        when(row.isNull("property")).thenReturn(true);
        final HashMap<Object,Object> emptyMap = new HashMap<>();
        when(pm.nullValueForCollectionAndMap()).thenReturn(emptyMap);

        Object actual = invoker.invokeOnRowForFields(row, pm);

        assertThat(actual).isSameAs(emptyMap);
    }

    @Test
    public void should_get_simple_value_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.SIMPLE);

        when(row.getString("property")).thenReturn("value");
        when(pm.forTranscoding().decode("value")).thenReturn("value");

        Object actual = invoker.invokeOnRowForFields(row, pm);

        assertThat(actual).isEqualTo("value");
    }

    @Test
    public void should_get_id_value_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.ID);

        when(row.getString("property")).thenReturn("value");
        when(pm.forTranscoding().decode("value")).thenReturn("value");
        Object actual = invoker.invokeOnRowForFields(row, pm);

        assertThat(actual).isEqualTo("value");
    }

    @Test
    public void should_return_null_when_no_value() throws Exception {
        when(pm.type()).thenReturn(PropertyType.SIMPLE);
        when(row.isNull("property")).thenReturn(true);

        assertThat(invoker.invokeOnRowForFields(row, pm)).isNull();
    }

    @Test
    public void should_exception_when_invoking_getter_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.SIMPLE);

        when(row.getString("property")).thenThrow(new RuntimeException(""));

        exception.expect(AchillesException.class);
        exception.expectMessage("Cannot retrieve property 'property' for entity class 'null' from CQL Row");

        invoker.invokeOnRowForFields(row, pm);
    }

    @Test
    public void should_exception_when_invoking_list_getter_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.LIST);

        when(row.getList("property", String.class)).thenThrow(new RuntimeException(""));

        exception.expect(AchillesException.class);
        exception.expectMessage("Cannot retrieve list property 'property' from CQL Row");

        invoker.invokeOnRowForList(row, pm, "property", String.class);
    }

    @Test
    public void should_exception_when_invoking_set_getter_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.SET);

        when(row.getSet("property", String.class)).thenThrow(new RuntimeException(""));

        exception.expect(AchillesException.class);
        exception.expectMessage("Cannot retrieve set property 'property' from CQL Row");

        invoker.invokeOnRowForSet(row, pm, "property", String.class);
    }

    @Test
    public void should_exception_when_invoking_map_getter_from_row() throws Exception {
        when(pm.type()).thenReturn(PropertyType.MAP);
        when(row.getMap("property", Integer.class, String.class)).thenThrow(new RuntimeException(""));

        exception.expect(AchillesException.class);
        exception.expectMessage("Cannot retrieve map property 'property' from CQL Row");

        invoker.invokeOnRowForMap(row, pm, "property", Integer.class, String.class);
    }

    @Test
    public void should_invoke_on_row_for_type() throws Exception {
        when(row.getString("column")).thenReturn("value");

        Object actual = invoker.invokeOnRowForType(row, String.class, "column");

        assertThat(actual).isEqualTo("value");
    }

    @Test
    public void should_test() throws Exception {
        List<Object> rawValues = new ArrayList<>(Collections.nCopies(2, null));

        assertThat(rawValues.get(0)).isNull();
        assertThat(rawValues.get(1)).isNull();
    }
}
