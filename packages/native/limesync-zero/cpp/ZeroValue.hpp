#pragma once

#include <NitroModules/JSIConverter.hpp>
#include <memory>

namespace limesync::zero {
struct Json;
}

namespace margelo::nitro::limesync {

using ZeroValue = std::shared_ptr<::limesync::zero::Json>;
using ZeroResult = std::variant<NullType, bool, std::string, double, ZeroValue>;

}

namespace margelo::nitro {

template <>
struct JSIConverter<limesync::ZeroValue> final {
  static limesync::ZeroValue fromJSI(jsi::Runtime& runtime, const jsi::Value& value);
  static jsi::Value toJSI(jsi::Runtime& runtime, const limesync::ZeroValue& value);
  static bool canConvert(jsi::Runtime&, const jsi::Value& value) { return value.isObject(); }
};

}
