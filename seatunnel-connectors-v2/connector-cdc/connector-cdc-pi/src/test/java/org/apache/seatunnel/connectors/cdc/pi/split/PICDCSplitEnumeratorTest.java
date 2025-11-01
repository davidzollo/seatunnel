package org.apache.seatunnel.connectors.cdc.pi.split;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfig;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for PICDCSplitEnumerator focusing on split assignment, checkpointing, and restoration
 * logic.
 */
public class PICDCSplitEnumeratorTest {

    @Test
    public void testSnapshotStateContainsAssignedAndPending() throws Exception {
        // 5 paths with maxWebIdsPerSplit=2 should create 3 splits
        List<String> paths = Arrays.asList("p1", "p2", "p3", "p4", "p5");
        PIConfigHelper helper = configHelperWithPaths(paths, 2);
        FakeContext ctx = new FakeContext(2, 0);
        PICDCSplitEnumerator enumerator = new PICDCSplitEnumerator(helper, ctx);

        enumerator.open();
        enumerator.registerReader(0);
        enumerator.run();

        PICDCCheckpointState snapshot = enumerator.snapshotState(1L);
        // Should have exactly 1 assigned split and 2 remaining splits
        int totalSplits =
                snapshot.getAssignedSplits().size() + snapshot.getRemainingSplits().size();
        Assertions.assertEquals(3, totalSplits, "Total splits should be 3 (5 paths / 2 per split)");
        Assertions.assertEquals(
                1, snapshot.getAssignedSplits().size(), "Should have exactly 1 assigned split");
        Assertions.assertEquals(
                2, snapshot.getRemainingSplits().size(), "Should have exactly 2 remaining splits");
    }

    @Test
    public void testAddSplitsBackMovesFromAssignedToPending() throws Exception {
        // 4 paths with maxWebIdsPerSplit=2 should create 2 splits
        List<String> paths = Arrays.asList("p1", "p2", "p3", "p4");
        PIConfigHelper helper = configHelperWithPaths(paths, 2);
        FakeContext ctx = new FakeContext(2, 0);
        PICDCSplitEnumerator enumerator = new PICDCSplitEnumerator(helper, ctx);

        enumerator.open();
        enumerator.registerReader(0);
        enumerator.run();
        PICDCCheckpointState before = enumerator.snapshotState(2L);
        // Should have 1 assigned and 1 remaining
        Assertions.assertEquals(
                1, before.getAssignedSplits().size(), "Should have exactly 1 assigned split");
        Assertions.assertEquals(
                1, before.getRemainingSplits().size(), "Should have exactly 1 remaining split");

        // Simulate reader failure and add its split back
        PICDCSplit returned = before.getAssignedSplits().get(0);
        enumerator.addSplitsBack(Collections.singletonList(returned), 0);

        PICDCCheckpointState after = enumerator.snapshotState(3L);
        // The returned split should not appear in assigned set now
        boolean stillAssigned =
                after.getAssignedSplits().stream()
                        .anyMatch(s -> s.splitId().equals(returned.splitId()));
        Assertions.assertFalse(stillAssigned, "Returned split should not be in assigned set");
        // After adding back, should have 0 assigned and 2 remaining
        Assertions.assertEquals(
                0,
                after.getAssignedSplits().size(),
                "Should have 0 assigned splits after adding back");
        Assertions.assertEquals(
                2,
                after.getRemainingSplits().size(),
                "Should have 2 remaining splits (1 original + 1 returned)");
    }

    @Test
    public void testRestoreFromCheckpointKeepsAssignedAndPending() throws Exception {
        // Build two splits with valid PI path format
        PICDCSplit s1 =
                new PICDCSplit(
                        "cdc-split-0", Arrays.asList("\\\\TestServer\\p1", "\\\\TestServer\\p2"));
        PICDCSplit s2 =
                new PICDCSplit(
                        "cdc-split-1", Arrays.asList("\\\\TestServer\\p3", "\\\\TestServer\\p4"));
        PICDCCheckpointState state =
                new PICDCCheckpointState(
                        Collections.singletonList(s2), // remaining
                        Collections.singletonList(s1) // assigned
                        );
        state.setCheckpointId(10L);

        // Build enumerator from checkpoint
        List<String> paths = Arrays.asList("p1", "p2", "p3", "p4");
        PIConfigHelper helper = configHelperWithPaths(paths, 2);
        FakeContext ctx = new FakeContext(2); // no readers needed for this assertion
        PICDCSplitEnumerator enumerator = new PICDCSplitEnumerator(helper, ctx, state);

        PICDCCheckpointState snap = enumerator.snapshotState(11L);
        // After restore, assigned splits are requeued to pending for re-dispatch
        Assertions.assertEquals(0, snap.getAssignedSplits().size());
        // Both s1 (previously assigned) and s2 (previously remaining) should be in pending
        Assertions.assertEquals(2, snap.getRemainingSplits().size());
    }

    @Test
    public void testDynamicReaderRegistration() throws Exception {
        // Test that readers can be registered dynamically after enumeration
        // 4 paths with maxWebIdsPerSplit=2 creates 2 splits
        List<String> paths = Arrays.asList("p1", "p2", "p3", "p4");
        PIConfigHelper helper = configHelperWithPaths(paths, 2);
        FakeContext ctx = new FakeContext(2); // Start with no readers
        PICDCSplitEnumerator enumerator = new PICDCSplitEnumerator(helper, ctx);

        enumerator.open();
        enumerator.run();

        // Verify no readers registered yet
        Assertions.assertEquals(
                0, ctx.registeredReaders().size(), "Should have no readers initially");

        // Dynamically register reader 0
        ctx.registerReader(0);
        enumerator.registerReader(0); // This assigns 1 split automatically

        // Verify reader 0 received initial split from registration
        Assertions.assertTrue(
                ctx.assignments.containsKey(0), "Reader 0 should have received splits");
        Assertions.assertEquals(
                1, ctx.assignments.get(0).size(), "Reader 0 should have 1 split from registration");

        // Request another split for reader 0
        enumerator.handleSplitRequest(0);

        // Verify reader 0 now has 2 splits (1 from registration + 1 from request)
        Assertions.assertEquals(
                2,
                ctx.assignments.get(0).size(),
                "Reader 0 should have 2 splits (1 from registration + 1 from request)");
        // Ensure splitIds are unique (no duplicate assignment)
        Set<String> uniqueIds = new HashSet<>();
        for (PICDCSplit s : ctx.assignments.get(0)) {
            uniqueIds.add(s.splitId());
        }
        Assertions.assertEquals(
                ctx.assignments.get(0).size(),
                uniqueIds.size(),
                "Split IDs must be unique for a given reader");

        // All splits exhausted, request again should signal no more splits
        enumerator.handleSplitRequest(0);
        Assertions.assertTrue(
                ctx.noMoreSplitsSignals.contains(0),
                "Should signal no more splits after all splits assigned");
    }

    @Test
    public void testReaderRequestBeforeEnumerationComplete() throws Exception {
        // Test the timing race condition: Reader requests split before enumeration completes
        List<String> paths = Arrays.asList("p1", "p2", "p3");
        PIConfigHelper helper = configHelperWithPaths(paths, 2);
        FakeContext ctx = new FakeContext(2, 0);
        PICDCSplitEnumerator enumerator = new PICDCSplitEnumerator(helper, ctx);

        enumerator.open();
        enumerator.registerReader(0);

        // Reader requests split BEFORE run() is called (enumeration not started yet)
        enumerator.handleSplitRequest(0);

        // Verify no "no more splits" signal was sent prematurely
        Assertions.assertEquals(
                0,
                ctx.noMoreSplitsSignals.size(),
                "Should not signal no more splits before enumeration completes");

        // Now run enumeration to generate splits
        enumerator.run();

        // Request again after enumeration
        enumerator.handleSplitRequest(0);

        // Verify splits were assigned
        Assertions.assertTrue(
                ctx.assignments.containsKey(0), "Reader 0 should have received splits");
        Assertions.assertTrue(
                ctx.assignments.get(0).size() > 0, "Reader 0 should have at least one split");

        // Exhaust all pending splits by requesting until queue is empty
        while (enumerator.currentUnassignedSplitSize() > 0) {
            enumerator.handleSplitRequest(0);
        }

        // One more request should trigger no-more-splits signal
        enumerator.handleSplitRequest(0);

        // Now should signal no more splits
        Assertions.assertTrue(
                ctx.noMoreSplitsSignals.contains(0),
                "Should signal no more splits after all splits are assigned");
    }

    /**
     * Helper to create PIConfigHelper with given paths and maxWebIdsPerSplit
     *
     * @param paths
     * @param maxWebIdsPerSplit
     * @return
     */
    private PIConfigHelper configHelperWithPaths(List<String> paths, int maxWebIdsPerSplit) {
        Map<String, Object> map = new HashMap<>();
        map.put(PIConfig.PI_WEB_API_URL.key(), "http://localhost");
        map.put(PIConfig.USERNAME.key(), "u");
        map.put(PIConfig.PASSWORD.key(), "p");
        // Convert simple path names to valid PI path format
        List<String> validPaths = new ArrayList<>();
        for (String path : paths) {
            validPaths.add("\\\\TestServer\\" + path);
        }
        map.put(PIConfig.PI_PATHS.key(), validPaths);
        map.put(PIConfig.MAX_WEBIDS_PER_SPLIT.key(), maxWebIdsPerSplit);
        ReadonlyConfig cfg = ReadonlyConfig.fromMap(map);
        return new PIConfigHelper(cfg);
    }

    /** A fake implementation of SourceSplitEnumerator.Context for testing purposes. */
    private static class FakeContext implements SourceSplitEnumerator.Context<PICDCSplit> {
        private final Set<Integer> readers = new HashSet<>();
        private final Map<Integer, List<PICDCSplit>> assignments = new HashMap<>();
        private final int parallelism;
        private final List<Integer> noMoreSplitsSignals = new ArrayList<>();

        FakeContext(int parallelism, Integer... initialReaders) {
            this.parallelism = parallelism;
            readers.addAll(Arrays.asList(initialReaders));
        }

        // Add method to dynamically register reader
        void registerReader(int subtaskId) {
            readers.add(subtaskId);
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return readers;
        }

        @Override
        public void assignSplit(int subtaskId, List<PICDCSplit> splits) {
            assignments.computeIfAbsent(subtaskId, k -> new ArrayList<>()).addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplitsSignals.add(subtask);
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
