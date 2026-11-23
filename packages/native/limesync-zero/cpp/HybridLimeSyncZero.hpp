#pragma once

#include "HybridOneZeroSpec.hpp"
#include "zero/Client.hpp"
#include <memory>
#include <mutex>
#include <string>

namespace margelo::nitro::limesync {

struct ZeroResponse {
  std::shared_ptr<const std::string> text;
};

class HybridLimeSyncZero : public HybridOneZeroSpec {
 public:
  HybridLimeSyncZero();
  ~HybridLimeSyncZero() override;
  std::shared_ptr<Promise<void>> open(const std::string& name, const std::string& schemaJson,
                                      const std::string& clientID) override;
  std::shared_ptr<Promise<void>> close() override;

 private:
  void loadHybridMethods() override;
  std::shared_ptr<Promise<ZeroResponse>> execute(std::string requestJson);
  struct State;
  std::shared_ptr<State> state_;
};

}  // namespace margelo::nitro::limesync
