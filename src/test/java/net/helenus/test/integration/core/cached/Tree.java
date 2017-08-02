package net.helenus.test.integration.core.cached;

import net.helenus.core.annotation.Cacheable;
import net.helenus.mapping.annotation.Column;
import net.helenus.mapping.annotation.PartitionKey;
import net.helenus.mapping.annotation.Table;

import java.util.UUID;

@Table
@Cacheable
public interface Tree {

    @PartitionKey
    UUID id();

    String name();

    Integer age();

    Integer height();

}
