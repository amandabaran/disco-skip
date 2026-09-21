#include <cstdlib>
#include <fstream>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

#include "device.hpp"

// OpenDevice definitions
namespace dory::ctrl {
OpenDevice::OpenDevice() = default;

OpenDevice::OpenDevice(struct ibv_device *device) : dev{device} {
  ctx = ibv_open_device(device);
  if (ctx == nullptr) {
    throw std::runtime_error("Could not get device list: " +
                             std::string(std::strerror(errno)));
  }

  if (ibv_query_device_ex(ctx, nullptr, &device_attr_ex) != 0) {
    throw std::runtime_error("Could not query device: " +
                             std::string(std::strerror(errno)));
  }
}

OpenDevice::~OpenDevice() {
  if (ctx != nullptr) {
    ibv_close_device(ctx);
  }
}

// Copy constructor
OpenDevice::OpenDevice(OpenDevice const &o) : dev{o.dev} {
  ctx = ibv_open_device(dev);
  if (ctx == nullptr) {
    throw std::runtime_error("Could not get device list: " +
                             std::string(std::strerror(errno)));
  }

  if (ibv_query_device_ex(ctx, nullptr, &device_attr_ex) != 0) {
    throw std::runtime_error("Could not query device: " +
                             std::string(std::strerror(errno)));
  }
}

// Move constructor
OpenDevice::OpenDevice(OpenDevice &&o) noexcept
    : dev{o.dev}, ctx{o.ctx}, device_attr_ex(o.device_attr_ex) {
  o.ctx = nullptr;
}

// Copy assignment operator
OpenDevice &OpenDevice::operator=(OpenDevice const &o) {
  if (&o == this) {
    return *this;
  }

  ctx = ibv_open_device(o.dev);
  if (ctx == nullptr) {
    throw std::runtime_error("Could not get device list: " +
                             std::string(std::strerror(errno)));
  }

  if (ibv_query_device_ex(ctx, nullptr, &device_attr_ex) != 0) {
    throw std::runtime_error("Could not query device: " +
                             std::string(std::strerror(errno)));
  }

  return *this;
}

// Move assignment operator
OpenDevice &OpenDevice::operator=(OpenDevice &&o) noexcept {
  if (&o == this) {
    return *this;
  }

  dev = o.dev;
  ctx = o.ctx;
  device_attr_ex = o.device_attr_ex;
  o.ctx = nullptr;

  return *this;
}

struct ibv_device_attr const &OpenDevice::deviceAttributes() const {
  return device_attr_ex.orig_attr;
}

struct ibv_device_attr_ex const &OpenDevice::extendedAttributes() const {
  return device_attr_ex;
}
}  // namespace dory::ctrl

// Device definitions
namespace dory::ctrl {
Devices::Devices() = default;

Devices::Devices(Devices &&o) noexcept : dev_list{o.dev_list} {
  o.dev_list = nullptr;
}

Devices &Devices::operator=(Devices &&o) noexcept {
  dev_list = o.dev_list;
  o.dev_list = nullptr;
  return *this;
}

Devices::~Devices() {
  if (dev_list != nullptr) {
    ibv_free_device_list(dev_list);
  }
}

std::vector<OpenDevice> &Devices::list(bool force) {
  if (force || dev_list == nullptr) {
    int num_devices = 0;
    dev_list = ibv_get_device_list(&num_devices);

    if (dev_list == nullptr) {
      throw std::runtime_error("Error getting device list: " +
                               std::string(std::strerror(errno)));
    }

    for (int i = 0; i < num_devices; i++) {
      devices.emplace_back(dev_list[i]);
    }

    if (devices.empty()) {
      throw std::runtime_error("No IB devices were found.");
    }
  }

  return devices;
}
}  // namespace dory::ctrl

namespace dory::ctrl {
ResolvedPort::ResolvedPort(OpenDevice &od)
    : open_dev{od}, port_index{-1}, port_id{0}, port_lid{0} {
  (void)port_index;
}


namespace {
/// Read one sysfs line, or "" when it cannot be read.
std::string readSysfsLine(std::string const &path) {
  std::ifstream f(path);
  std::string v;
  if (f) {
    std::getline(f, v);
  }
  return v;
}

/// An all-zero GID is an empty table slot, not an address.
bool gidIsZero(union ibv_gid const &g) {
  for (int i = 0; i < 16; ++i) {
    if (g.raw[i] != 0) {
      return false;
    }
  }
  return true;
}

/// ::ffff:a.b.c.d -- an IPv4 address mapped into the GID, which is the one
/// that corresponds to the experiment LAN rather than to link-local discovery.
bool gidIsIpv4Mapped(union ibv_gid const &g) {
  for (int i = 0; i < 10; ++i) {
    if (g.raw[i] != 0) {
      return false;
    }
  }
  return g.raw[10] == 0xff && g.raw[11] == 0xff;
}
}  // namespace

int ResolvedPort::selectRoceGid(uint8_t port, int gid_tbl_len) {
  if (char const *forced = std::getenv("DS_ROCE_GID_INDEX")) {
    int const idx = std::atoi(forced);
    std::cout << "RoCE GID index " << idx << " forced by DS_ROCE_GID_INDEX"
              << std::endl;
    return idx;
  }

  // name(), NOT devName(). ibv_device::name is the IB device ("mlx5_2") and
  // dev_name is the character device ("uverbs2"); only the former exists under
  // /sys/class/infiniband. Using devName() made every read here return the
  // empty string, so the "RoCE v2" test was false for EVERY entry, scoring
  // collapsed to "is it IPv4-mapped", and the first such entry won -- which on
  // the r650 nodes is index 2, the RoCE **v1** GID. It still connected,
  // because both ends chose the same wrong entry, so the only symptom was an
  // empty type in the log: "RoCE GID index 2 ( on )". v1 is L2-only and is not
  // what a routed or ECN-tuned fabric wants.
  std::string const base = std::string("/sys/class/infiniband/") +
                           open_dev.name() + "/ports/" +
                           std::to_string(static_cast<int>(port));

  int best = -1;
  int best_score = -1;
  for (int i = 0; i < gid_tbl_len; ++i) {
    union ibv_gid g {};
    if (ibv_query_gid(open_dev.context(), port, i, &g) != 0) {
      continue;
    }
    if (gidIsZero(g)) {
      continue;
    }
    std::string const type =
        readSysfsLine(base + "/gid_attrs/types/" + std::to_string(i));
    bool const v2 = type.find("v2") != std::string::npos;
    bool const v4 = gidIsIpv4Mapped(g);

    // RoCE v2 on the IPv4 address is what a modern routed fabric wants; a v1
    // or link-local entry is a usable last resort rather than a peer.
    int const score = (v2 ? 2 : 0) + (v4 ? 1 : 0);
    if (score > best_score) {
      best_score = score;
      best = i;
    }
  }

  if (best >= 0 && best_score < 2) {
    // Reachable only when NO entry reported RoCE v2 -- either a v1-only
    // fabric or an unreadable sysfs. Say so, because the previous version of
    // this function took that path silently.
    std::cout << "RoCE GID: no v2 entry found; falling back to index " << best
              << ". If this is unexpected, check "
              << "/sys/class/infiniband/" << open_dev.name()
              << "/ports/" << static_cast<int>(port) << "/gid_attrs/types/"
              << std::endl;
  }
  if (best >= 0) {
    std::string const type =
        readSysfsLine(base + "/gid_attrs/types/" + std::to_string(best));
    std::string const ndev =
        readSysfsLine(base + "/gid_attrs/ndevs/" + std::to_string(best));
    std::cout << "RoCE GID index " << best << " (" << type << " on " << ndev
              << ")" << std::endl;
  }
  return best;
}

bool ResolvedPort::bindTo(size_t index) {
  size_t skipped_active_ports = 0;
  for (uint8_t i = 1; i <= open_dev.deviceAttributes().phys_port_cnt; i++) {
    struct ibv_port_attr port_attr = {};

    if (ibv_query_port(open_dev.context(), i, &port_attr)) {
      throw std::runtime_error("Failed to query port: " +
                               std::string(std::strerror(errno)));
    }

    if (port_attr.phys_state != IBV_PORT_ACTIVE &&
        port_attr.phys_state != IBV_PORT_ACTIVE_DEFER) {
      continue;
    }

    if (skipped_active_ports == index) {
      // ── ETHERNET IS ACCEPTED NOW, AS RoCE ─────────────────────────────
      //
      // This used to throw on anything but InfiniBand, which made the whole
      // stack IB-only: the r650 nodes present ACTIVE 100G ports whose link
      // layer is Ethernet, and every run died here before bootstrap. IB is
      // unchanged below -- LID is still resolved and still what the
      // connection layer uses on an IB port. RoCE additionally needs a GID,
      // because an Ethernet port has no meaningful LID.
      port_id = i;
      port_lid = port_attr.lid;
      link_layer = port_attr.link_layer;
      active_mtu = port_attr.active_mtu;

      if (link_layer == IBV_LINK_LAYER_ETHERNET) {
        gid_index = selectRoceGid(i, port_attr.gid_tbl_len);
        if (gid_index < 0 ||
            ibv_query_gid(open_dev.context(), i, gid_index, &port_gid) != 0) {
          throw std::runtime_error(
              "RoCE port " + std::to_string(static_cast<int>(i)) +
              " has no usable GID. A RoCE port is addressed by GID, so there "
              "is nothing to connect with; check the port's IP configuration.");
        }
      } else if (link_layer != IBV_LINK_LAYER_INFINIBAND) {
        throw std::runtime_error(
            "Port link layer is " + linkLayerStr(port_attr.link_layer) +
            ", which is neither InfiniBand nor Ethernet/RoCE");
      }

      return true;
    }

    skipped_active_ports += 1;
  }

  return false;
}
}  // namespace dory::ctrl
