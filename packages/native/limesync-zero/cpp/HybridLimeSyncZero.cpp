#include "HybridLimeSyncZero.hpp"
#include "ZeroStorageDirectory.hpp"
#include "zero/Json.hpp"
#include <algorithm>
#include <filesystem>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace margelo::nitro {
namespace {
using ::limesync::zero::Json;
using ::limesync::zero::PackedObject;

Json decode(jsi::Runtime& runtime, auto&& value, const jsi::Function& keys,
             unsigned depth) {
  if (depth > 128) throw std::invalid_argument("JSON nesting exceeds 128 levels");
  if (value.isUndefined() || value.isNull()) return nullptr;
  if (value.isBool()) return value.getBool();
  if (value.isNumber()) {
    double number = value.getNumber();
    if (!std::isfinite(number))
      throw std::invalid_argument("Native Zero values must contain finite numbers");
    return number == 0 ? 0 : number;
  }
  if (value.isString()) return std::forward<decltype(value)>(value).getString(runtime).utf8(runtime);
  if (value.isObject()) {
    auto object = std::forward<decltype(value)>(value).getObject(runtime);
    if (object.isArray(runtime)) {
      auto array = std::move(object).getArray(runtime);
      auto size = array.size(runtime);
      Json::Array result;
      result.reserve(size);
      for (size_t i = 0; i < size; ++i)
        result.push_back(decode(runtime, array.getValueAtIndex(runtime, i), keys, depth + 1));
      return result;
    }
    if (!object.isFunction(runtime)) {
      auto names = keys.call(runtime, object).getObject(runtime).getArray(runtime);
      PackedObject result;
      auto size = names.size(runtime);
      result.fields.reserve(std::min(size, size_t{64}));
      for (size_t i = 0; i < size; ++i) {
        auto key = names.getValueAtIndex(runtime, i).getString(runtime);
        auto member = object.getProperty(runtime, key);
        if (!member.isUndefined())
          result.fields.emplace_back(key.utf8(runtime), decode(runtime, std::move(member), keys, depth + 1));
      }
      return result;
    }
  }
  throw std::invalid_argument("Native Zero requires JSON values");
}

void materializeRequest(Json& request) {
  if (auto* packed = std::get_if<PackedObject>(&request.value)) {
    Json::Object fields;
    for (auto& [key, member] : packed->fields) {
      auto [field, inserted] = fields.try_emplace(std::move(key), std::move(member));
      if (inserted) materializeRequest(field->second);
    }
    request.value = std::move(fields);
  } else if (auto* items = std::get_if<Json::Array>(&request.value)) {
    for (auto& item : *items) materializeRequest(item);
  }
}

using FieldNames = std::unordered_map<std::string_view, jsi::PropNameID>;

jsi::Value encode(jsi::Runtime& runtime, const Json& value, FieldNames& names) {
  if (value.null()) return jsi::Value::null();
  if (auto boolean = std::get_if<bool>(&value.value)) return jsi::Value(*boolean);
  if (auto number = std::get_if<double>(&value.value)) return jsi::Value(*number);
  if (auto text = std::get_if<std::string>(&value.value))
    return jsi::String::createFromUtf8(runtime, *text);
  if (auto items = std::get_if<Json::Array>(&value.value)) {
    auto result = jsi::Array(runtime, items->size());
    for (size_t i = 0; i < items->size(); ++i)
      result.setValueAtIndex(runtime, i, encode(runtime, (*items)[i], names));
    return result;
  }
  const auto& fields = value.fields();
  auto result = jsi::Object(runtime);
  // preserve __proto__ as an own data property, like JSON.parse.
  const bool hasPrototypeField = fields.contains("__proto__");
  jsi::Value prototype;
  if (hasPrototypeField) {
    prototype = result.getPrototype(runtime);
    result.setPrototype(runtime, jsi::Value::null());
  }
  for (const auto& [key, field] : fields) {
    auto member = encode(runtime, field, names);
    const auto name = names.find(key);
    if (name != names.end()) {
      result.setProperty(runtime, name->second, member);
    } else {
      auto property = jsi::PropNameID::forUtf8(runtime, key);
      result.setProperty(runtime, property, member);
      if (names.size() < 64) names.emplace(key, std::move(property));
    }
  }
  if (hasPrototypeField) result.setPrototype(runtime, prototype);
  return result;
}
}

limesync::ZeroValue JSIConverter<limesync::ZeroValue>::fromJSI(jsi::Runtime& runtime, const jsi::Value& value) {
  auto keys = runtime.global()
                  .getPropertyAsObject(runtime, "Object")
                  .getPropertyAsFunction(runtime, "keys");
  return std::make_shared<Json>(decode(runtime, value, keys, 0));
}

jsi::Value JSIConverter<limesync::ZeroValue>::toJSI(jsi::Runtime& runtime, const limesync::ZeroValue& value) {
  if (const auto* serialized = std::get_if<Json::Serialized>(&value->value)) {
    const auto& json = serialized->text;
    return jsi::Value::createFromJsonUtf8(
        runtime, reinterpret_cast<const uint8_t*>(json.data()), json.size());
  }
  FieldNames names;
  return encode(runtime, *value, names);
}
}

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
                                                   ZeroValue schema,
                                                   const std::string& clientID) {
  if (name.empty() || name.size() > 128 ||
      name.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") !=
          std::string::npos) {
    throw std::invalid_argument(
        "native Zero name must contain 1-128 letters, "
        "digits, hyphens or underscores");
  }
  return Promise<void>::async([state = state_, name, schema = std::move(schema), clientID]() {
    std::string canonicalPath =
        canonicalizePath(zeroStorageDirectory() + "/zero-" + name + ".sqlite");
    std::lock_guard<std::mutex> guard(state->mutex);
    if (state->client) throw std::runtime_error("native Zero client is already open");
    if (!tryRegisterOpenPath(canonicalPath)) {
      throw std::runtime_error("native Zero store is already open: " + canonicalPath);
    }
    try {
      materializeRequest(*schema);
      state->client = std::make_unique<::limesync::zero::Client>(canonicalPath, *schema, clientID);
      state->canonicalPath = canonicalPath;
    } catch (...) {
      unregisterOpenPath(canonicalPath);
      state->client.reset();
      state->canonicalPath.clear();
      throw;
    }
  });
}

std::shared_ptr<Promise<ZeroResult>> HybridLimeSyncZero::execute(ZeroValue request) {
  return Promise<ZeroResult>::async([state = state_, request = std::move(request)]() -> ZeroResult {
    std::unique_lock<std::mutex> guard(state->mutex);
    if (!state->client) throw std::runtime_error("native Zero client is closed");
    materializeRequest(*request);
    auto result = state->client->executeValue(*request);
    guard.unlock();
    if (result.object() || result.array()) result.value = Json::Serialized{result.dump(true)};
    return std::make_shared<Json>(std::move(result));
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
