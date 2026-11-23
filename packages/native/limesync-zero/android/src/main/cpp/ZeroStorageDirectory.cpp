#include <fbjni/fbjni.h>

#include "ZeroStorageDirectory.hpp"

#include <stdexcept>

namespace margelo::nitro::limesync {

std::string zeroStorageDirectory() {
  static const auto directoryClass =
      facebook::jni::findClassStatic("com/margelo/nitro/limesync/ZeroStorageDirectory");
  static const auto path =
      directoryClass->getStaticMethod<facebook::jni::local_ref<facebook::jni::JString>()>("path");
  auto directory = path(directoryClass);
  if (!directory) throw std::runtime_error("E_ZERO_STORAGE_OPEN: React context is not ready");
  return directory->toStdString();
}

}  // namespace margelo::nitro::limesync
