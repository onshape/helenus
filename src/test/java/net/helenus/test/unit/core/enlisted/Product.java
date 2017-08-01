/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package net.helenus.test.unit.core.enlisted;

import java.util.Date;
import java.util.List;
import java.util.Random;

import net.helenus.core.Auditor;
import net.helenus.mapping.annotation.*;


class CurrentUserProvider implements Auditor {
    public String getCurrentAuditor() {
        return "unknown";
    }
}

@InheritedTable
interface AbstractProduct {
    static final Random rnd = new Random();

    @PartitionKey
    default Long id() {
        return rnd.nextLong();
    }
}

abstract class AbstractProductImpl implements AbstractProduct {
    public String anotherMethodHere() { return "a string."; }
}

@Table
@Enlisted(AbstractProductImpl.class)
@Audited(CurrentUserProvider.class)
public interface Product extends AbstractProduct {

    @ClusteringColumn
    Date time();
    //@DefaultValue(false)
    boolean discounted();
    String description();
    List<String> reviews();
}

// An "enlisted" model/entity object is one that implements the draft/builder pattern under the covers.  The
// following is the code that is generated
/*
final class EnlistedProduct extends AbstractProductImpl implements Product, MapExportable {

    private final Long id;
    public Long id() { return id; }
    public Long getId() { return id; }

    private final Date time;
    public Date time() { return time; }
    public Date getTime() { return time; }

    private final boolean discounted;
    public boolean discounted() { return discounted; }
    public boolean getDiscounted() {return discounted; }
    public boolean isDiscounted() { return discounted; }

    private final String description;
    public String description() { return description; }
    public String getDescription() { return description; }

    public final ImmutableList<String> reviews;
    public ImmutableList<String> reviews() { return reviews; }
    public ImmutableList<String> getReviews() { return reviews; }

    EnlistedProduct(Long id, Date time, boolean discounted, String description, List<String> reviews) {
        this.id = id;
        this.time = time;
        this.discounted = discounted;
        this.description = description;
        this.reviews = ImmutableList.copyOf(reviews);
    }

    private EnlistedProduct() { id = null; time = null; discounted = false; description = null; reviews = null; }
    static Draft draft() { return new EnlistedProduct().new Draft(); }

    // Use to create a draft which is necessary when updating an existing Product instance.
    Draft update() { return new Draft(this); }

    class Draft extends AbstractHelenusDraft<Product> {

        Draft() {
            super(null);

            // Primary Keys:
            set("id", id());

            // Defaults for this entity object are set here:
            set("discounted", defaults.get("discounted")); // TODO: when we create a MappingEntity have a map we can clone with default values
        }

        Draft(Product entity) {
            super((MapExportable) entity);

            // Add primary key components to map.
            set("id", entity.id());
        }

        public Draft setTime(Date time) {
            mutate("time", time);
            return this;
        }

        public Draft setDiscounted(boolean discounted) {
            mutate("discounted", discounted);
            return this;
        }

        public Draft setDescription(String description) {
            mutate("description", description);
            return this;
        }

        public Draft setReviews(List<String> reviews) {
            mutate("reviews", reviews); //TODO: think more about this, and counters...
            return this;
        }

        TODO:
        toMap
        equals
        hashCode
        toString
    }
}
*/
