#pragma once

#include "HybridOneZeroSpec.hpp"
#include "zero/Client.hpp"
#include <memory>
#include <mutex>
#include <string>

namespace margelo::nitro::limesync {

class HybridLimeSyncZero : public HybridOneZeroSpec {
 public:
  HybridLimeSyncZero();
  ~HybridLimeSyncZero() override;
  std::shared_ptr<Promise<void>> open(const std::string& name, ZeroValue schema,
                                      const std::string& clientID) override;
  std::shared_ptr<Promise<ZeroResult>> execute(ZeroValue request) override;
  std::shared_ptr<Promise<void>> close() override;

 private:
  struct State;
  std::shared_ptr<State> state_;
};

}  // namespace margelo::nitro::limesync
