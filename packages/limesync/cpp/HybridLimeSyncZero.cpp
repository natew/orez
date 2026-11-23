#include "HybridLimeSyncZero.hpp"
#include "ZeroStorageDirectory.hpp"
#include <filesystem>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>

namespace margelo::nitro::limesync {

namespace {
std::mutex openPathsMutex;
std::set<std::string> openPaths;

std::string canonicalizePath(const std::string& raw) {
  try {
    return std::filesystem::weakly_canonical(raw).string();
  } catch (...) {
    return std::filesystem::absolute(raw).lexically_normal().string();
  }
}

bool tryRegisterOpenPath(const std::string& path) {
  std::lock_guard<std::mutex> lock(openPathsMutex);
  return openPaths.insert(path).second;
}

void unregisterOpenPath(const std::string& path) {
  if (path.empty()) return;
  std::lock_guard<std::mutex> lock(openPathsMutex);
  openPaths.erase(path);
}
}  // namespace

struct HybridLimeSyncZero::State {
  std::mutex mutex;
  std::string canonicalPath;
  std::unique_ptr<::limesync::zero::Client> client;
  ~State() {
    if (!canonicalPath.empty()) {
      unregisterOpenPath(canonicalPath);
      canonicalPath.clear();
    }
  }
};

HybridLimeSyncZero::HybridLimeSyncZero() : HybridObject(TAG), state_(std::make_shared<State>()) {}
HybridLimeSyncZero::~HybridLimeSyncZero() = default;

std::shared_ptr<Promise<void>> HybridLimeSyncZero::open(const std::string& name,
                                                   const std::string& schemaJson,
                                                   const std::string& clientID) {
  if (name.empty() || name.size() > 128 ||
      name.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") !=
          std::string::npos) {
    throw std::invalid_argument(
        "native Zero name must contain 1-128 letters, "
        "digits, hyphens or underscores");
  }
  std::string rawPath = zeroStorageDirectory() + "/zero-" + name + ".sqlite";
  std::string canonicalPath = canonicalizePath(rawPath);

  return Promise<void>::async([state = state_, canonicalPath, schemaJson, clientID]() {
    std::lock_guard<std::mutex> guard(state->mutex);
    if (state->client) throw std::runtime_error("native Zero client is already open");
    if (!tryRegisterOpenPath(canonicalPath)) {
      throw std::runtime_error("native Zero store is already open: " + canonicalPath);
    }
    try {
      state->client = std::make_unique<::limesync::zero::Client>(canonicalPath, schemaJson, clientID);
      state->canonicalPath = canonicalPath;
    } catch (...) {
      unregisterOpenPath(canonicalPath);
      state->client.reset();
      state->canonicalPath.clear();
      throw;
    }
  });
}

std::shared_ptr<Promise<std::string>> HybridLimeSyncZero::execute(const std::string& requestJson) {
  return Promise<std::string>::async([state = state_, requestJson]() {
    std::lock_guard<std::mutex> guard(state->mutex);
    if (!state->client) throw std::runtime_error("native Zero client is closed");
    return state->client->execute(requestJson);
  });
}

std::shared_ptr<Promise<void>> HybridLimeSyncZero::close() {
  return Promise<void>::async([state = state_]() {
    std::lock_guard<std::mutex> guard(state->mutex);
    std::string path = state->canonicalPath;
    state->canonicalPath.clear();
    unregisterOpenPath(path);
    if (state->client) {
      try {
        state->client->close();
      } catch (...) {
        state->client.reset();
        throw;
      }
      state->client.reset();
    }
  });
}

}  // namespace margelo::nitro::limesync
