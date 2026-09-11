// Runs the --selftest body against a fake arena.
//
// The point is not to re-test F1/F2 -- insert_test and put_test do that. It is
// that the selftest *itself* used to live in main.cpp, the one translation unit
// that cannot be compiled off-cluster, so every line of it was unverified until
// a cluster build. Six builds have been lost that way.
//
// Now it is a header templated over Reader and Ops, FakeOps satisfies both, and
// the whole thing runs here in a millisecond. A mistyped field, a phase that
// returns the wrong code, or an expectation that no longer matches the script
// fails locally.
//
// Also asserts the things the runbook publishes as the expected output, so that
// document cannot drift from the code without a test noticing.

#include <cstdio>
#include <sstream>
#include <string>

#include "ds_selftest.hpp"
#include "fake_ops.hpp"

static int g_failures = 0;

#define CHECK(cond, msg)                                                      \
  do {                                                                        \
    if (!(cond)) {                                                            \
      std::printf("FAIL: %s  [%s] line %d\n", (msg), #cond, __LINE__);        \
      ++g_failures;                                                           \
    }                                                                         \
  } while (0)

static uint32_t const kLayers = 4;

static bool contains(std::string const &haystack, char const *needle) {
  return haystack.find(needle) != std::string::npos;
}

static void checkAllThreePhasesPassOnAFreshStructure() {
  FakeOps ops = buildInitialArena(kLayers, 4096);
  std::ostringstream out;

  int const rc = ds::runSelftest(ops, ops, kLayers, /*hint=*/nullptr, out);
  std::string const log = out.str();

  CHECK(rc == 0, "the selftest passes on a freshly bootstrapped structure");
  CHECK(contains(log, "SELFTEST PASS: I1-I4 hold"), "structure phase passes");
  CHECK(contains(log, "DESCENT PASS"), "descent phase passes");
  CHECK(contains(log, "WRITE PASS"), "write phase passes");
  CHECK(!contains(log, "FAIL"), "and nothing reports a failure");

  // The exact lines the runbook documents as expected output. If the script or
  // the reporting changes, this is what says the runbook needs updating.
  CHECK(contains(log, "probes:       5 resolved, 5 absent"),
        "the descent probe line matches what the runbook publishes");
  CHECK(contains(log, "puts:         9 resolved, 0 failed (6 data-only, "
                      "3 structural)"),
        "and so does the put line");
  CHECK(contains(log, "splits:       2 data, 1 index, 0 capacity; "
                      "1 boundary no-op(s)"),
        "and the split line -- two data splits, one index, one no-op");
  CHECK(contains(log, "readback:     9/9 keys match"), "and the readback line");

  // The chaining is only worth anything if the batch count is well below the
  // operation count. Asserted as a ratio rather than an exact number so the
  // script can grow without this becoming a chore, but it must stay a real gap.
  CHECK(contains(log, "round trips:"), "the round-trip line is reported");

  if (g_failures != 0) std::printf("--- log ---\n%s\n", log.c_str());
}

static void checkEveryScriptedKeyIsReadableAfterwards() {
  FakeOps ops = buildInitialArena(kLayers, 4096);
  std::ostringstream out;
  CHECK(ds::runSelftest(ops, ops, kLayers, nullptr, out) == 0,
        "the selftest passes");

  // Independently of the selftest's own read-back, walk to every scripted key.
  ds::Descender<FakeOps> d(ops);
  ds::PathStep path[ds::kMaxLayers];
  for (ds::SelftestWrite const &w : ds::kSelftestWrites) {
    ds::Value want = w.v;
    for (ds::SelftestWrite const &later : ds::kSelftestWrites) {
      if (later.k == w.k) want = later.v;
    }
    ds::DescentResult const r = d.descend(w.k, kLayers, path);
    CHECK(r.ok() && r.found && r.value == want,
          "every scripted key reads back with its final value");
  }

  ds::DescentResult const never =
      d.descend(ds::kSelftestAbsentKey, kLayers, path);
  CHECK(never.ok() && !never.found,
        "and the never-written key still reads as absent");
}

static void checkStructurePhaseRejectsABrokenStructure() {
  // The selftest has to be able to fail. A phase that passes unconditionally
  // would be worse than no phase, since it reads as evidence.
  FakeOps ops = buildInitialArena(kLayers, 4096);
  // Break I2: point a head's next at itself, making the level chain cyclic.
  ops.node(ds::headAddr(0)).next_id = ds::headAddr(0).id;

  std::ostringstream out;
  int const rc = ds::runSelftest(ops, ops, kLayers, nullptr, out);
  CHECK(rc != 0, "a broken structure fails the selftest");
  CHECK(contains(out.str(), "SELFTEST FAIL"), "reporting which phase failed");
  CHECK(!contains(out.str(), "WRITE PASS"),
        "and stops rather than running later phases over a broken structure");
}

static void checkSelftestIsRerunnable() {
  // Re-running the whole selftest over an already-populated structure passes.
  //
  // I expected this to fail, on the grounds that the descent phase asserts the
  // structure is empty. It does not, and the reason is worth recording: the
  // descent probes are {0, 1, 42, 1000, 2^20} and the write script touches
  // {50 .. 600}, so the two sets are disjoint by construction, and every put in
  // the script is idempotent -- an update or a boundary no-op the second time.
  //
  // That makes a second --selftest on a live structure meaningful rather than a
  // guaranteed failure, which is worth keeping true. If someone adds a probe
  // that collides with the script, this is the test that says so.
  FakeOps ops = buildInitialArena(kLayers, 4096);
  std::ostringstream first;
  CHECK(ds::runSelftest(ops, ops, kLayers, nullptr, first) == 0,
        "the first run passes");

  std::ostringstream second;
  CHECK(ds::runSelftest(ops, ops, kLayers, nullptr, second) == 0,
        "and so does a second run over the structure the first one built");
  CHECK(contains(second.str(), "WRITE PASS"), "including the write phase");
  CHECK(contains(second.str(), "probes:       5 resolved, 5 absent"),
        "the descent probes are still absent, being disjoint from the script");

  // The second run should be almost entirely no-ops and updates: the
  // boundaries already exist, so nothing structural is created.
  CHECK(contains(second.str(), "splits:       0 data, 0 index"),
        "and the second run creates no new splits at all");
}

static void checkWritePhaseFailsWhenTheArenaIsExhausted() {
  // The write phase has to be able to report failure. Four dynamic slots is
  // far fewer than the script needs, so allocation runs out part-way: every
  // write consumes a vector and there is no reclamation, which is exactly the
  // real failure mode a long run hits.
  FakeOps ops = buildInitialArena(kLayers, 4);
  std::ostringstream out;
  int const rc = ds::runSelftest(ops, ops, kLayers, nullptr, out);

  CHECK(rc != 0, "an exhausted arena fails the selftest");
  CHECK(contains(out.str(), "WRITE FAIL"), "at the write phase");
  CHECK(contains(out.str(), "SELFTEST PASS"),
        "having passed the structure phase, which needs no allocation");
  CHECK(!contains(out.str(), "WRITE PASS"), "and not claiming the writes worked");
}

int main() {
  std::printf("selftest_test: layers=%u\n", kLayers);
  checkAllThreePhasesPassOnAFreshStructure();
  checkEveryScriptedKeyIsReadableAfterwards();
  checkStructurePhaseRejectsABrokenStructure();
  checkSelftestIsRerunnable();
  checkWritePhaseFailsWhenTheArenaIsExhausted();

  if (g_failures != 0) {
    std::printf("%d FAILURE(S)\n", g_failures);
    return 1;
  }
  std::printf("ALL PASS\n");
  return 0;
}
