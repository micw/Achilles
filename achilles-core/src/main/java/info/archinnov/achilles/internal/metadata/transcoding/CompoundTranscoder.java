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
package info.archinnov.achilles.internal.metadata.transcoding;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;

public class CompoundTranscoder extends AbstractTranscoder {

    private static final Logger log = LoggerFactory.getLogger(CompoundTranscoder.class);

    public CompoundTranscoder(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public List<Object> encodeToComponents(PropertyMeta idMeta, Object compoundKey, boolean onlyStaticColumns) {
        log.trace("Encode {} to CQL components", compoundKey);
        List<Object> compoundComponents = new ArrayList<>();
        List<Class<?>> componentClasses = idMeta.getComponentClasses();
        List<Field> componentFields;
        if (compoundKey != null) {
            if (onlyStaticColumns) {
                componentFields = idMeta.getPartitionComponentFields();
            } else {
                componentFields = idMeta.getComponentFields();
            }
            for (int i = 0; i < componentFields.size(); i++) {
                Object component = invoker.getValueFromField(compoundKey, componentFields.get(i));
                Object encoded = super.encodeInternal(componentClasses.get(i), component);
                compoundComponents.add(encoded);
            }
        }
        return compoundComponents;
    }

    @Override
    public List<Object> encodeToComponents(PropertyMeta pm, List<?> components) {
        log.trace("Encode {} to CQL components", components);
        List<Class<?>> componentClasses = pm.getComponentClasses();
        String embeddedClassName = pm.getValueClass().getCanonicalName();

        return encodeComponents(components, componentClasses, embeddedClassName);
    }

    public List<Object> encodeComponents(List<?> components, List<Class<?>> componentClasses, String embeddedClassName) {
        List<Object> encodedComponents = new ArrayList<>();
        for (int i=0; i<components.size(); i++) {
            Object component = components.get(i);
            Class<?> targetComponentClass = componentClasses.get(i);
            if (component != null) {
                Class<?> componentClass = component.getClass();
                Validator.validateTrue(targetComponentClass.equals(componentClass), "The component '%s' for embedded id '%s' has an unknown type. Valid type is '%s'",
                        component, embeddedClassName, targetComponentClass.getCanonicalName());
                Object encoded = super.encodeInternal(targetComponentClass, component);
                encodedComponents.add(encoded);
            }
        }
        return encodedComponents;
    }

    @Override
    public Object decodeFromComponents(PropertyMeta idMeta, List<?> components) {
        log.trace("Decode from CQL components", components);
        List<Field> componentFields = idMeta.getComponentFields();

        List<Object> decodedComponents = new ArrayList<>();
        List<Class<?>> componentClasses = idMeta.getComponentClasses();
        for (int i = 0; i < components.size(); i++) {
            Object decoded = super.decodeInternal(componentClasses.get(i), components.get(i));
            decodedComponents.add(decoded);
        }

        Object compoundKey;
        compoundKey = injectValues(idMeta, decodedComponents, componentFields);
        return compoundKey;
    }

    private Object injectValues(PropertyMeta pm, List<?> components, List<Field> componentFields) {
        log.trace("Instantiate primary compound key from CQL components {}", components);
        Object compoundKey = pm.instantiate();

        for (int i = 0; i < components.size(); i++) {
            Object compValue = components.get(i);
            invoker.setValueToField(compoundKey, componentFields.get(i), compValue);
        }
        return compoundKey;
    }
}
