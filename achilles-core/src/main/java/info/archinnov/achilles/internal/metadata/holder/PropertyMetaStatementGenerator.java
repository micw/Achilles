package info.archinnov.achilles.internal.metadata.holder;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Optional;
import info.archinnov.achilles.type.Pair;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PropertyMetaStatementGenerator extends PropertyMetaView{

    PropertyMetaStatementGenerator(PropertyMeta meta) {
        super(meta);
    }

    public Insert prepareInsertPrimaryKey(Insert insert) {
        if (meta.structure().isEmbeddedId()) {
            return meta.embeddedIdProperties.prepareInsertPrimaryKey(insert);
        } else {
            return insert.value(meta.getCQL3PropertyName(), bindMarker(meta.getCQL3PropertyName()));
        }
    }

    public RegularStatement prepareWhereClauseForSelect(Optional<PropertyMeta> pmO, Select from) {
        if (meta.structure().isEmbeddedId()) {
            return meta.embeddedIdProperties.prepareWhereClauseForSelect(pmO, from);
        } else {
            return from.where(eq(meta.getCQL3PropertyName(), bindMarker(meta.getCQL3PropertyName())));
        }
    }

    public RegularStatement prepareWhereClauseForDelete(boolean onlyStaticColumns, Delete mainFrom) {
        if (meta.structure().isEmbeddedId()) {
            return meta.embeddedIdProperties.prepareWhereClauseForDelete(onlyStaticColumns, mainFrom);
        } else {
            return mainFrom.where(eq(meta.getCQL3PropertyName(), bindMarker(meta.getCQL3PropertyName())));
        }
    }

    public Update.Assignments prepareUpdateField(Update.Conditions updateConditions) {
        String property = meta.getCQL3PropertyName();
        return updateConditions.with(set(property, bindMarker(property)));
    }

    public Update.Assignments prepareUpdateField(Update.Assignments assignments) {
        String property = meta.getCQL3PropertyName();
        return assignments.and(set(property, bindMarker(property)));
    }

    public Update.Where prepareCommonWhereClauseForUpdate(Update.Assignments assignments, boolean onlyStaticColumns) {
        if (meta.structure().isEmbeddedId()) {
            return meta.embeddedIdProperties.prepareCommonWhereClauseForUpdate(assignments, onlyStaticColumns);
        } else {
            String idName = meta.getCQL3PropertyName();
            return assignments.where(eq(idName, bindMarker(idName)));
        }
    }

    public Pair<Update.Where, Object[]> generateWhereClauseForUpdate(Object entity, PropertyMeta pm, Update.Assignments update) {
        Object primaryKey = meta.getPrimaryKey(entity);
        if (meta.structure().isEmbeddedId()) {
            return meta.embeddedIdProperties.generateWhereClauseForUpdate(primaryKey, pm, update);
        } else {
            Object id = meta.forTranscoding().encodeToCassandra(primaryKey);
            Update.Where where = update.where(eq(meta.getCQL3PropertyName(), id));
            Object[] boundValues = new Object[] { id };
            return Pair.create(where, boundValues);
        }
    }

    public Select.Selection prepareSelectField(Select.Selection select) {
        if (meta.structure().isEmbeddedId()) {
            for (String component : meta.embeddedIdProperties.getCQL3ComponentNames()) {
                select = select.column(component);
            }
            return select;
        } else {
            return select.column(meta.getCQL3PropertyName());
        }
    }


    // DirtyCheckChangeSet
    public Update.Assignments generateUpdateForRemoveAll(Update.Conditions conditions) {
        String propertyName = meta.getCQL3PropertyName();
        return conditions.with(set(propertyName, bindMarker(propertyName)));
    }

    public Update.Assignments generateUpdateForAddedElements(Update.Conditions conditions) {
        String propertyName = meta.getCQL3PropertyName();
        return conditions.with(addAll(propertyName, bindMarker(propertyName)));
    }

    public Update.Assignments generateUpdateForRemovedElements(Update.Conditions conditions) {
        String propertyName = meta.getCQL3PropertyName();
        return conditions.with(removeAll(propertyName, bindMarker(propertyName)));
    }

    public Update.Assignments generateUpdateForAppendedElements(Update.Conditions conditions) {
        String propertyName = meta.getCQL3PropertyName();
        return conditions.with(appendAll(propertyName, bindMarker(propertyName)));
    }

    public Update.Assignments generateUpdateForPrependedElements(Update.Conditions conditions) {
        String propertyName = meta.getCQL3PropertyName();
        return conditions.with(prependAll(propertyName, bindMarker(propertyName)));
    }

    public Update.Assignments generateUpdateForRemoveListElements(Update.Conditions conditions) {
        String propertyName = meta.getCQL3PropertyName();
        return conditions.with(discardAll(propertyName, bindMarker(propertyName)));
    }

    public Update.Assignments generateUpdateForAddedEntries(Update.Conditions conditions) {
        String propertyName = meta.getCQL3PropertyName();
        return conditions.with(putAll(propertyName, bindMarker(propertyName)));
    }

    public Update.Assignments generateUpdateForRemovedKey(Update.Conditions conditions) {
        String propertyName = meta.getCQL3PropertyName();
        return conditions.with(put(propertyName, bindMarker("key"), bindMarker("nullValue")));
    }

    public Update.Assignments generateUpdateForSetAtIndexElement(Update.Conditions conditions, int index, Object encoded) {
        String propertyName = meta.getCQL3PropertyName();
        return conditions.with(setIdx(propertyName, index, encoded));
    }

    public Update.Assignments generateUpdateForRemovedAtIndexElement(Update.Conditions conditions, int index) {
        String propertyName = meta.getCQL3PropertyName();
        return conditions.with(setIdx(propertyName, index, null));
    }
}
