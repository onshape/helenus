package net.helenus.test.integration.core.cached;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.utils.UUIDs;
import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.operation.PreparedOperation;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import net.helenus.test.integration.core.simple.User;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static net.helenus.core.Query.eq;
import static net.helenus.core.Query.marker;

public class SessionCacheTest extends AbstractEmbeddedCassandraTest {

    static HelenusSession session;
    static Tree tree;

    static Random rnd = new Random();
    static Set<UUID> keys = new HashSet<UUID>();

    @BeforeClass
    public static void beforeTest() {
        session = Helenus.init(getSession()).showCql().add(Tree.class).autoCreateDrop().showCql().get();
        tree = Helenus.dsl(Tree.class, session.getMetadata());

        PreparedOperation<ResultSet> insertOp =
                session.insert()
                        .value(tree::id, marker())
                        .value(tree::name, marker())
                        .value(tree::age, marker())
                        .value(tree::height, marker())
                        .prepare();

        for (int i = 0; i < 10; i++) {
            UUID key = UUIDs.random();
            keys.add(key);
            insertOp.bind(key,
                    UUIDs.random().toString(),
                    rnd.nextInt(1000),
                    rnd.nextInt(5000))
                    .sync();
        }
        Assert.assertEquals(session.count(tree).sync().intValue(), 10);
    }

    @Test
    public void cacheThingsTest() throws Exception {
        keys.forEach(key -> {
            Optional<Tree> aTree = session.select(Tree.class)
                    .where(tree::id, eq(key))
                    .single()
                    .sync();
            Assert.assertTrue(aTree.isPresent());
            Assert.assertEquals(session.fetch(key.toString()), aTree.get());
        });
    }

}
