#pragma once

#include <fstream>
#include <iostream>
#include <ostream>
#include <string>

namespace ds {

/// Where THIS client's report goes.
///
/// A client is a client whether it was launched as a process or as a thread,
/// so its log has to look the same either way: the harness reads one
/// client<N>.txt per client and sums the `Local tput` lines out of them, and
/// wait_for_logs in experiments/compare/lib.sh waits for the ###DONE### that
/// each client writes as its last line. One file per process would have broken
/// every parser and the run-completion check with it.
///
/// Process-wide stdout cannot do that, so the client path writes to a
/// thread-local stream instead. It defaults to std::cout, which is exactly
/// today's behaviour when a process hosts one client and invoker.sh tees its
/// output -- so the single-threaded path is unchanged and does not depend on
/// this being wired up.
inline std::ostream *&clientOutPtr() {
    static thread_local std::ostream *p = &std::cout;
    return p;
}

/// The current thread's client log.
inline std::ostream &clientOut() { return *clientOutPtr(); }

/// Point this thread's client output at `os` for the duration of the scope,
/// restoring the previous target on the way out.
///
/// Scoped rather than a bare setter because a thread that threw partway
/// through would otherwise leave a dangling pointer to a destroyed ofstream,
/// and the next write would be a use-after-free rather than an error.
class ScopedClientOut {
public:
    explicit ScopedClientOut(std::ostream &os) : prev_{clientOutPtr()} {
        clientOutPtr() = &os;
    }
    ~ScopedClientOut() { clientOutPtr() = prev_; }

    ScopedClientOut(ScopedClientOut const &) = delete;
    ScopedClientOut &operator=(ScopedClientOut const &) = delete;

private:
    std::ostream *prev_;
};

/// The log file a client writes, named exactly as the harness expects.
///
/// run.sh names the tmux window client$c and invoker.sh tees to
/// $LOG_DIR/$FOLDER/client$c.txt, where c is the 1-based client number --
/// proc_id minus the server count. Deriving it the same way here keeps the
/// filenames identical whether a client was a process or a thread, which is
/// the whole point.
inline std::string clientLogPath(std::string const &dir, uint64_t proc_id,
                                 uint64_t num_servers) {
    return dir + "/client" + std::to_string(proc_id - num_servers) + ".txt";
}

}  // namespace ds
