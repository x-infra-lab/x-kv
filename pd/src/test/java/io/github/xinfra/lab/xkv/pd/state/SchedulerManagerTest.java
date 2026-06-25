package io.github.xinfra.lab.xkv.pd.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulerManagerTest {

    private SchedulerManager mgr;

    @BeforeEach
    void setUp() {
        mgr = new SchedulerManager();
    }

    @Test
    void registerAndList() {
        var state = new InMemoryPdStateMachine();
        var controller = new OperatorControllerImpl(5, 600_000);
        var scheduler = new LeaderBalanceScheduler(state, controller, 60_000);

        mgr.register("leader-balance", scheduler);

        var list = mgr.list();
        assertThat(list).hasSize(1);
        assertThat(list.get(0).name()).isEqualTo("leader-balance");
        assertThat(list.get(0).running()).isTrue();
        assertThat(list.get(0).paused()).isFalse();

        scheduler.close();
    }

    @Test
    void pauseAndResume() {
        var state = new InMemoryPdStateMachine();
        var controller = new OperatorControllerImpl(5, 600_000);
        var scheduler = new LeaderBalanceScheduler(state, controller, 60_000);

        mgr.register("leader-balance", scheduler);

        assertThat(mgr.pause("leader-balance")).isTrue();
        assertThat(mgr.getStatus("leader-balance").paused()).isTrue();
        assertThat(scheduler.isPaused()).isTrue();

        assertThat(mgr.resume("leader-balance")).isTrue();
        assertThat(mgr.getStatus("leader-balance").paused()).isFalse();
        assertThat(scheduler.isPaused()).isFalse();

        scheduler.close();
    }

    @Test
    void pauseNonexistentReturnsFalse() {
        assertThat(mgr.pause("nonexistent")).isFalse();
        assertThat(mgr.resume("nonexistent")).isFalse();
    }

    @Test
    void unregisterRemovesEntry() {
        var state = new InMemoryPdStateMachine();
        var controller = new OperatorControllerImpl(5, 600_000);
        var scheduler = new LeaderBalanceScheduler(state, controller, 60_000);

        mgr.register("leader-balance", scheduler);
        assertThat(mgr.size()).isEqualTo(1);

        mgr.unregister("leader-balance");
        assertThat(mgr.size()).isEqualTo(0);
        assertThat(mgr.getStatus("leader-balance")).isNull();

        scheduler.close();
    }

    @Test
    void unregisterAll() {
        var state = new InMemoryPdStateMachine();
        var controller = new OperatorControllerImpl(5, 600_000);
        var s1 = new LeaderBalanceScheduler(state, controller, 60_000);
        var s2 = new RegionBalanceScheduler(state, controller, 60_000);

        mgr.register("leader-balance", s1);
        mgr.register("region-balance", s2);
        assertThat(mgr.size()).isEqualTo(2);

        mgr.unregisterAll();
        assertThat(mgr.size()).isEqualTo(0);

        s1.close();
        s2.close();
    }

    @Test
    void listSortedByName() {
        var state = new InMemoryPdStateMachine();
        var controller = new OperatorControllerImpl(5, 600_000);
        var s1 = new RegionBalanceScheduler(state, controller, 60_000);
        var s2 = new LeaderBalanceScheduler(state, controller, 60_000);

        mgr.register("region-balance", s1);
        mgr.register("leader-balance", s2);

        var list = mgr.list();
        assertThat(list).hasSize(2);
        assertThat(list.get(0).name()).isEqualTo("leader-balance");
        assertThat(list.get(1).name()).isEqualTo("region-balance");

        s1.close();
        s2.close();
    }
}
