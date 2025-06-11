package org.apache.shardingsphere.mode.repository.standalone.memory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class MemoryRepositoryTest {

    MemoryRepository memoryRepository = new MemoryRepository();

    @BeforeEach
    void setUp() {
        memoryRepository.persist("/metadata/sharding_db/schemas/sharding_db/tables/t_order/versions/0", "t_order");
        memoryRepository.persist("/metadata/sharding_db/schemas/sharding_db/tables/t_user/versions/0", "t_user");
        memoryRepository.persist("/metadata/encrypt_db/schemas/encrypt_db/tables/t_encrypt_01/versions/0", "t_encrypt_01");
        memoryRepository.persist("/metadata/encrypt_db/schemas/encrypt_db/tables/t_encrypt_02/versions/0", "t_encrypt_02");
    }

    @Test
    void assertPersist() {
        memoryRepository.persist("testKey", "testValue");
        assertThat(memoryRepository.query("testKey"), is("testValue"));
    }

    @Test
    void assertUpdate() {
        memoryRepository.update("/metadata/sharding_db/schemas/sharding_db/tables/t_order/versions/0", "t_order_updated");
        assertThat(memoryRepository.query("/metadata/sharding_db/schemas/sharding_db/tables/t_order/versions/0"),
                is("t_order_updated"));
    }

    @Test
    void assertDelete() {
        memoryRepository.delete("/metadata/sharding_db/schemas/sharding_db/tables/t_order/versions/0");
        assertThat(memoryRepository.isExisted("/metadata/sharding_db/schemas/sharding_db/tables/t_order/versions/0"), is(false));
        assertThat(memoryRepository.query("/metadata/sharding_db/schemas/sharding_db/tables/t_order/versions/0"), is((String) null));
    }

    @Test
    void assertGetChildrenKeys() {
        assertThat(memoryRepository.getChildrenKeys("/metadata").size(), is(2));
        assertThat(memoryRepository.getChildrenKeys("/metadata").containsAll(Arrays.asList("encrypt_db", "sharding_db")), is(true));
        assertThat(memoryRepository.getChildrenKeys("/metadata/sharding_db/schemas/sharding_db/tables/").size(), is(2));
        assertThat(memoryRepository.getChildrenKeys("/metadata/encrypt_db/schemas/encrypt_db/tables/").size(), is(2));
    }
}
