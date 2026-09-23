#pragma once

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <infiniband/verbs.h>

#include <dory/ctrl/device.hpp>

/// Which RDMA device to open, decided at RUN TIME.
///
/// (Byte-identical in disco-skip, swarm-kv and fusee, which are separate conan
/// packages with no shared include path. `diff` the three must be empty.)
///
/// ── WHY THIS IS NOT A CONSTANT ────────────────────────────────────────────
///
/// It used to be, three different ways, and all three were wrong the moment a
/// second cluster appeared:
///
///   disco-skip      available_devices[0]         -- with a comment saying
///                                                   "this corresponds to the
///                                                   3rd device (mlx5_2)",
///                                                   which index 0 is not
///   swarm-kv/fusee  Devices().list().back()      -- the LAST device
///
/// On the r320 nodes there is exactly one device, mlx4_0, so every rule picked
/// it and the disagreement was invisible. The r650 nodes have four:
///
///   mlx5_0  eno12399np0      Active  25G    management network
///   mlx5_1  eno12409np1      Down
///   mlx5_2  enp202s0f0np0    Active  100G   carries 10.10.1.x -- the one we want
///   mlx5_3  enp202s0f1np1    Down           second port of the same card
///
/// So `.back()` picks mlx5_3, which is DOWN, and the competitors fail outright.
/// That is the SAFE failure. Index 0 picks mlx5_0, which is ACTIVE -- so
/// disco-skip runs, reports nothing wrong, and pushes every RDMA operation
/// over a 25G management link instead of the 100G experiment LAN. A comparison
/// against the r320 nodes would then show the faster hardware as barely
/// faster, and the cause would not appear anywhere in the logs.
///
/// ── THE RULE ─────────────────────────────────────────────────────────────
///
/// Prefer a port whose state is ACTIVE; among those, the highest rate. That
/// resolves correctly on both clusters with no per-cluster configuration,
/// which matters because the two clusters share one binary built on the
/// gateway: mlx4_0 is the only active device on r320, and mlx5_2 is the
/// fastest active one on r650.
///
/// DS_RDMA_DEVICE overrides it, by device name ("mlx5_2") or by index ("2"),
/// for the case where the heuristic is wrong -- e.g. deliberately measuring
/// the slower fabric. An override that does not match is a hard error rather
/// than a silent fallback, because falling back to a different NIC than the
/// one asked for is exactly the failure this file exists to prevent.
namespace dory::rdmasel {

/// Gb/s for an ibv port, from the speed/width encoding. Used only to rank
/// candidates, so the approximation in FDR10/FDR is harmless.
inline double portRate(ibv_port_attr const &a) {
  double per_lane = 0.0;
  switch (a.active_speed) {
    case 1:   per_lane = 2.5;     break;  // SDR
    case 2:   per_lane = 5.0;     break;  // DDR
    case 4:   per_lane = 10.0;    break;  // QDR
    case 8:   per_lane = 10.3125; break;  // FDR10
    case 16:  per_lane = 14.0625; break;  // FDR
    case 32:  per_lane = 25.0;    break;  // EDR
    case 64:  per_lane = 50.0;    break;  // HDR
    case 128: per_lane = 100.0;   break;  // NDR
    default:  per_lane = 0.0;     break;
  }
  int lanes = 0;
  switch (a.active_width) {
    case 1:  lanes = 1;  break;  // 1X
    case 2:  lanes = 4;  break;  // 4X
    case 4:  lanes = 8;  break;  // 8X
    case 8:  lanes = 12; break;  // 12X
    // 2X IS NOT IN THE OBVIOUS PLACE IN THIS ENCODING. The IBTA width codes
    // are a bitmask whose first four values are 1X, 4X, 8X, 12X; 2X was added
    // later and took the NEXT BIT, 16, rather than slotting in between 1X and
    // 4X. Omitting it is what made this function return 0 for the r650 nodes'
    // 100G port: mlx5_2 reports active_speed 64 (50 Gbps/lane) and
    // active_width 16 (2X), so the rate is 2 x 50 = 100G -- but with 16
    // unhandled the lane count fell to 0, the port ranked below a 25G link,
    // and the selector chose the management NIC while reporting it as the
    // best ACTIVE device. Confirmed against the netdev: enp202s0f0np0 reads
    // 100000 Mb/s, eno12399np0 reads 25000.
    case 16: lanes = 2;  break;  // 2X
    default: lanes = 0;  break;
  }
  return per_lane * static_cast<double>(lanes);
}

/// Link rate in Gb/s from the backing netdev, or 0 if it cannot be read.
///
/// A BACKSTOP FOR THE TABLE ABOVE, because these ports are RoCE: link_layer is
/// Ethernet and the netdev is the authority on speed, while the IB
/// speed/width encoding is a lossy second-hand account of it that has grown
/// new codes over time. One missing code silently demoted a 100G port to 0G,
/// so when the table cannot explain an ACTIVE port, ask the netdev instead of
/// ranking it last.
inline double netdevRate(std::string const &ibdev, uint8_t port) {
  auto slurp = [](std::string const &path) -> std::string {
    std::ifstream f(path);
    std::string v;
    if (f) std::getline(f, v);
    return v;
  };
  std::string const base = "/sys/class/infiniband/" + ibdev + "/ports/" +
                           std::to_string(static_cast<int>(port));
  std::string const nd = slurp(base + "/gid_attrs/ndevs/0");
  if (nd.empty()) return 0.0;
  std::string const mbps = slurp("/sys/class/net/" + nd + "/speed");
  if (mbps.empty()) return 0.0;
  try {
    double const m = std::stod(mbps);
    return m > 0 ? m / 1000.0 : 0.0;   // Mb/s -> Gb/s
  } catch (...) {
    return 0.0;
  }
}

/// A FAILED PORT QUERY IS NOT A DOWN LINK, AND CONFLATING THEM COST A NIGHT.
///
/// This loop used to `continue` when ibv_query_port failed, so a device whose
/// ports could not be queried surveyed identically to one whose ports were
/// genuinely down: `[0]mlx4_0 down 0G`, then "no RDMA device has an ACTIVE
/// port. Check the fabric". On 2026-09-22 that message appeared four times on
/// w4 while `ibstat` reported State: Active, Rate: 56 and the port's
/// link_downed counter read 0 on a freshly booted host -- and in the fusee
/// logs it appeared SECONDS AFTER the same process had opened the device,
/// registered an RDMA buffer and connected its queue pairs. A link that was
/// down could not have done that. It sent us looking at the fabric, then
/// power-cycling two machines, for a transient query failure.
///
/// So: retry the query, and record the two cases separately. `ports_seen` is
/// how many ports answered; `ports_failed` how many never did. A survey with
/// ports_seen == 0 and ports_failed > 0 is "could not ask", which pickDevice
/// reports as such rather than blaming the cable.
constexpr int kPortQueryAttempts = 4;
constexpr int kPortQueryRetryMs = 25;

struct Candidate {
  size_t index = 0;
  std::string name;
  bool active = false;
  double rate = 0.0;
  unsigned ports_seen = 0;    ///< ports ibv_query_port actually answered for
  unsigned ports_failed = 0;  ///< ports that failed every attempt
  int last_errno = 0;         ///< errno from the last failed attempt
};

/// Describe every device, in list order. Never throws: a device that cannot be
/// queried is reported as inactive rather than aborting the survey, so one bad
/// NIC does not hide the good one.
inline std::vector<Candidate> surveyDevices(
    std::vector<ctrl::OpenDevice> &devices) {
  std::vector<Candidate> out;
  for (size_t i = 0; i < devices.size(); ++i) {
    Candidate c;
    c.index = i;
    // name() ("mlx5_2"), not devName() ("uverbs2"): this string is used to
    // build a /sys/class/infiniband path in netdevRate, where only the IB
    // device name exists. It is also the name ibstat prints, so the log lines
    // match what a human would check by hand.
    c.name = devices[i].name() ? devices[i].name() : "?";
    auto *ctx = devices[i].context();
    if (ctx != nullptr) {
      ibv_device_attr dev_attr{};
      if (ibv_query_device(ctx, &dev_attr) == 0) {
        // Ports are 1-based. Take the best port on the device: a card with one
        // active and one down port (mlx5_2/mlx5_3 are one card) must rank by
        // the port that actually carries traffic.
        for (uint8_t p = 1; p <= dev_attr.phys_port_cnt; ++p) {
          ibv_port_attr pa{};
          // Retry rather than give up: the failures observed were transient,
          // on a device the same process had just used successfully.
          int qrc = -1;
          for (int attempt = 0; attempt < kPortQueryAttempts; ++attempt) {
            qrc = ibv_query_port(ctx, p, &pa);
            if (qrc == 0) break;
            c.last_errno = errno;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kPortQueryRetryMs));
          }
          if (qrc != 0) { ++c.ports_failed; continue; }
          ++c.ports_seen;
          bool const act = (pa.state == IBV_PORT_ACTIVE);
          double r = portRate(pa);
          // `r <= 0.0`, not `r == 0.0`: the build runs -Werror=float-equal,
          // and "no rate we could work out" is the same case as a negative
          // one anyway.
          if (!(r > 0.0)) r = netdevRate(c.name, p);
          if (act && (!c.active || r > c.rate)) { c.active = true; c.rate = r; }
          else if (!c.active && r > c.rate)     { c.rate = r; }
        }
      }
    }
    out.push_back(c);
  }
  return out;
}

/// Say WHICH failure this is. "Check the fabric" is the right advice for a
/// down link and the wrong advice for a query that never got an answer, and
/// the old message gave it unconditionally.
inline std::string errmsg(std::vector<Candidate> const &survey) {
  bool any_seen = false, any_failed = false;
  int err = 0;
  for (auto const &c : survey) {
    if (c.ports_seen > 0) any_seen = true;
    if (c.ports_failed > 0) { any_failed = true; err = c.last_errno; }
  }
  if (!any_seen && any_failed) {
    return std::string(
        "ibv_query_port failed on every port of every device after ") +
        std::to_string(kPortQueryAttempts) + " attempts (errno " +
        std::to_string(err) + " " + std::strerror(err) +
        "). This is NOT a down link -- the ports were never successfully "
        "read. Check for a device opened twice in one process, or an "
        "exhausted fd/resource limit, before looking at the fabric.";
  }
  return "no RDMA device has an ACTIVE port. Check the fabric before running: "
         "every arm needs it, and a down link is not a slow link.";
}

/// Index of the device to open. Prints the survey and the choice, because a
/// wrong NIC is invisible in every other line of the log.
inline size_t pickDevice(std::vector<ctrl::OpenDevice> &devices) {
  if (devices.empty()) {
    throw std::runtime_error("no RDMA devices present");
  }
  auto const survey = surveyDevices(devices);

  std::cout << "RDMA devices:";
  for (auto const &c : survey) {
    char const *what = c.active            ? " ACTIVE "
                       : (c.ports_seen > 0) ? " down "
                                            : " UNQUERYABLE ";
    std::cout << " [" << c.index << "]" << c.name << what << c.rate << "G";
    if (c.ports_seen == 0 && c.ports_failed > 0) {
      std::cout << "(" << c.ports_failed << " port(s) failed ibv_query_port, "
                << "errno " << c.last_errno << " " << std::strerror(c.last_errno)
                << ")";
    }
  }
  std::cout << std::endl;

  if (char const *want = std::getenv("DS_RDMA_DEVICE")) {
    std::string const w{want};
    for (auto const &c : survey) {
      if (c.name == w || w == std::to_string(c.index)) {
        std::cout << "RDMA device: " << c.name << " (index " << c.index
                  << ") -- forced by DS_RDMA_DEVICE" << std::endl;
        if (!c.active) {
          std::cout << "RDMA device: WARNING " << c.name
                    << " is not ACTIVE; this will not carry traffic"
                    << std::endl;
        }
        return c.index;
      }
    }
    throw std::runtime_error("DS_RDMA_DEVICE=" + w +
                             " matches no device; refusing to fall back to a "
                             "different NIC than the one requested");
  }

  size_t best = 0;
  bool found = false;
  for (auto const &c : survey) {
    if (!c.active) continue;
    if (!found || c.rate > survey[best].rate) { best = c.index; found = true; }
  }
  if (!found) {
    throw std::runtime_error(
        errmsg(survey));
  }
  std::cout << "RDMA device: " << survey[best].name << " (index " << best
            << ", " << survey[best].rate << "G, ACTIVE)" << std::endl;
  return best;
}


/// Open the device pickDevice() chose.
///
/// DEFINED HERE, ONCE. It was first written as an anonymous-namespace helper
/// inside both client_index.hpp and server_index.hpp, which land in the same
/// translation unit and so collided:
///   error: redefinition of 'dory::race::{anonymous}::pickRdmaDevice()'
/// One inline definition in the shared header is the fix.
inline ctrl::OpenDevice openBestDevice() {
  // COPY, DO NOT MOVE. `devs` is static, so one device list serves the whole
  // process -- and std::move'ing the chosen entry OUT of it left a moved-from
  // OpenDevice behind: OpenDevice's move ctor sets `o.ctx = nullptr`. The
  // SECOND call in the same process then surveyed a list whose best entry had
  // a null context, never entered the port loop at all (ports_seen == 0 AND
  // ports_failed == 0), printed the device as UNQUERYABLE 0G and threw "no
  // RDMA device has an ACTIVE port. Check the fabric".
  //
  // It was never the fabric. That message cost 2026-09-22 two power cycles
  // and several hours: link_downed read 0 on a freshly booted host while
  // ibstat reported Active/56G, and the fusee log showed the throw arriving
  // THIRTY-SIX SECONDS AFTER the same process had opened the device,
  // registered an RDMA buffer and connected its queue pairs.
  //
  // Only fusee hit it, because only fusee calls this twice per process
  // (race/client_index.hpp and race/server_index.hpp each open their own).
  // disco-skip calls pickDevice on its own list in main.cpp and never comes
  // here, which is why it was immune and why this looked workload-specific.
  //
  // The copy ctor re-opens the device (ibv_open_device) and throws if that
  // fails, so every caller gets its own live context and the static list
  // keeps valid contexts and valid ibv_device pointers for the next call.
  static ctrl::Devices devs;
  auto &list = devs.list();
  return list.at(pickDevice(list));
}

}  // namespace dory::rdmasel
