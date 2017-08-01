package net.helenus.core;

import net.helenus.core.reflect.MapExportable;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

abstract class AbstractHelenusDraft<T> implements MapExportable {

    private final Map<String, Object> map = new HashMap<String, Object>();
    private final MapExportable entity;
    private final Auditor auditor;

    AbstractHelenusDraft(MapExportable entity) {
        this.entity = entity;
        this.auditor = null;
    }

    AbstractHelenusDraft(MapExportable entity , Auditor auditor) {
        this.entity = entity;
        this.auditor = auditor;
        audit(entity);
    }

    public <T> T build(Class<T> entityClass) {
        return Helenus.map(entityClass, map);
    }

    private void audit(MapExportable entity) {
        if (auditor != null) {
            String who = auditor.getCurrentAuditor();
            Date now = auditor.now();

            if (entity == null) {
                map.put("createdBy", who);
                map.put("createdAt", now);
            } else {
                map.put("modifiedBy", who);
                map.put("modifiedAt", now);
            }
        }
    }

    protected Object set(String key, Object value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        map.put(key, value);
        return value;
    }

    protected Object mutate(String key, Object value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        if (entity != null) {
            Map<String, Object> map = entity.toMap();

            if (map.containsKey(key) && !value.equals(map.get(key))) {
                this.map.put(key, value);
                return value;
            }
            return map.get(key);
        } else {
            map.put(key, value);
            return null;
        }

    }

    protected Object remove(String key) {
        Object value = map.get(key);
        map.put(key, null);
        return value;
    }

    @Override
    public Map<String, Object> toMap() {
        return map;
    }

    @Override
    public String toString() {
        return map.toString();
    }

}
