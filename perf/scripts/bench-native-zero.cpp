// from the repo root:
// c++ -O2 -std=c++20 -I packages/native/limesync-zero/cpp/zero \
//   perf/scripts/bench-native-zero.cpp packages/native/limesync-zero/cpp/zero/Client.cpp \
//   -lsqlite3 -o /tmp/bench-native-zero
// /tmp/bench-native-zero /path/to/new/scratch.sqlite

#include "Client.hpp"
#include "Json.hpp"

#include <sqlite3.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>

using limesync::zero::Client;
using limesync::zero::Json;
using Clock = std::chrono::steady_clock;

Json run(Client& client, Json request) { return client.executeValue(request); }

Json predicate(const char* field, const char* op, Json value) {
  return Json::Object{{"type", "simple"}, {"op", op},
                      {"left", Json::Object{{"type", "column"}, {"name", field}}},
                      {"right", Json::Object{{"type", "literal"}, {"value", std::move(value)}}}};
}

Json ordered(const char* table, Json where, Json::Array order, int limit) {
  return Json::Object{{"table", table}, {"where", std::move(where)},
                      {"orderBy", std::move(order)}, {"limit", limit}};
}

Json put(const char* table, Json row) {
  return Json::Object{{"op", "put"}, {"tableName", table}, {"value", std::move(row)}};
}

Json session(int i, int time) {
  return put("session_roster", Json::Object{
    {"id", "s" + std::to_string(i)}, {"state", i % 4 ? "running" : "exited"},
    {"kind", i % 15 ? "worker" : "summarizer"}, {"last_state_change_ms", time},
    {"name", std::string(100, 's')},
  });
}

Json message(int i, int time) {
  return put("message", Json::Object{
    {"id", "m" + std::to_string(i)}, {"session_id", "s" + std::to_string(i % 50 + 1)},
    {"seq", i / 50}, {"ts_ms", time}, {"body", std::string(400, 'm')},
  });
}

Json task(int i, int time) {
  return put("task_assignment", Json::Object{
    {"id", "t" + std::to_string(i)}, {"status", i % 3 ? "done" : "in_progress"},
    {"hot", i % 13 == 0}, {"updated_at", time}, {"title", std::string(80, 't')},
  });
}

Json beginPull(Client& client, Json::Array patches, int cookie) {
  Json response = Json::Object{{"cookie", cookie}, {"rowsPatch", std::move(patches)}};
  return run(client, Json::Object{{"op", "beginPull"}, {"response", response.dump()}});
}

struct Times {
  std::vector<double> samples;

  Json measure(Client& client, Json request) {
    auto start = Clock::now();
    auto result = client.executeValue(request);
    samples.push_back(std::chrono::duration<double, std::milli>(Clock::now() - start).count());
    return result;
  }

  void print(const char* label) {
    std::sort(samples.begin(), samples.end());
    std::cout << label << " p50=" << samples[samples.size() / 2]
              << " ms p95=" << samples[samples.size() * 95 / 100] << " ms\n";
  }
};

int main(int argc, char** argv) {
  if (argc != 2 || std::filesystem::exists(argv[1]))
    throw std::runtime_error("provide a fresh SQLite database path");
  auto schema = Json::parse(R"({"tables":{
    "session_roster":{"primaryKey":["id"],"columns":{"id":{"type":"string"},"state":{"type":"string"},"kind":{"type":"string"},"last_state_change_ms":{"type":"number"},"name":{"type":"string"}}},
    "message":{"primaryKey":["id"],"columns":{"id":{"type":"string"},"session_id":{"type":"string"},"seq":{"type":"number"},"ts_ms":{"type":"number"},"body":{"type":"string"}}},
    "task_assignment":{"primaryKey":["id"],"columns":{"id":{"type":"string"},"status":{"type":"string"},"hot":{"type":"boolean"},"updated_at":{"type":"number"},"title":{"type":"string"}}}
  }})");
  Client client(argv[1], schema, "bench");
  Json::Array seed;
  for (int i = 1; i <= 150; ++i) seed.push_back(session(i, i));
  for (int i = 1; i <= 1000; ++i) seed.push_back(message(i, i));
  for (int i = 1; i <= 500; ++i) seed.push_back(task(i, i));
  auto initial = beginPull(client, std::move(seed), 1);
  run(client, Json::Object{{"op", "commitPull"}, {"transaction", initial.at("transaction")}});
  auto queries = Json::parse(R"([
    {"table":"session_roster","where":{"type":"and","conditions":[
      {"type":"simple","op":"!=","left":{"type":"column","name":"kind"},"right":{"type":"literal","value":"summarizer"}},
      {"type":"or","conditions":[
        {"type":"simple","op":"<","left":{"type":"column","name":"state"},"right":{"type":"literal","value":"exited"}},
        {"type":"simple","op":">","left":{"type":"column","name":"state"},"right":{"type":"literal","value":"exited"}}]}]},
      "orderBy":[["last_state_change_ms","desc"]],"limit":4096},
    {"table":"session_roster","where":{"type":"and","conditions":[
      {"type":"simple","op":"!=","left":{"type":"column","name":"kind"},"right":{"type":"literal","value":"summarizer"}},
      {"type":"simple","op":"=","left":{"type":"column","name":"state"},"right":{"type":"literal","value":"exited"}},
      {"type":"simple","op":">=","left":{"type":"column","name":"last_state_change_ms"},"right":{"type":"literal","value":100}}]},
      "orderBy":[["last_state_change_ms","desc"]],"limit":40}
  ])").items();
  for (int i = 1; i <= 5; ++i) {
    queries.push_back(ordered("message", predicate("session_id", "=", "s" + std::to_string(i)),
                              {Json::Array{"seq", "desc"}, Json::Array{"ts_ms", "desc"},
                               Json::Array{"id", "asc"}}, 200));
  }
  queries.push_back(ordered("task_assignment", predicate("status", "IN", Json::Array{"open", "in_progress", "blocked"}),
                            {Json::Array{"updated_at", "desc"}}, 4096));
  queries.push_back(ordered("task_assignment", predicate("hot", "=", true),
                            {Json::Array{"updated_at", "desc"}}, 4096));

  Times registerWatch;
  Json::Array desired;
  const std::vector<size_t> expected{105, 12, 20, 20, 20, 20, 20, 166, 38};
  for (size_t i = 0; i < queries.size(); ++i) {
    auto watch = registerWatch.measure(client, Json::Object{{"op", "watch"}, {"ast", queries[i]},
                                                            {"id", static_cast<double>(i + 1)}});
    const auto& result = watch.at("results").items().at(0);
    if (result.at("value").items().size() != expected.at(i))
      throw std::runtime_error("incorrect row count for watch " + std::to_string(i));
    desired.push_back(Json::Object{{"hash", std::to_string(i)}, {"ast", queries[i]}});
  }
  run(client, Json::Object{{"op", "publish"}});
  Times apply, commit, publish, setQueries, mutation;
  for (int i = 0; i < 170; ++i) {
    auto response = Json(Json::Object{{"cookie", i + 2}, {"lastMutationIDChanges", Json::Object{{"bench", i}}}, {"rowsPatch",
        Json::Array{session(4, 2000 + i), message(1000, 2000 + i), task(39, 2000 + i)}}}).dump();
    const bool warmup = i < 20;
    auto execute = [&](Times& times, Json request) {
      return warmup ? run(client, std::move(request)) : times.measure(client, std::move(request));
    };
    auto pull = execute(apply, Json::Object{{"op", "beginPull"}, {"response", std::move(response)}});
    execute(commit, Json::Object{{"op", "commitPull"}, {"transaction", pull.at("transaction")}});
    auto updates = execute(publish, Json::Object{{"op", "publish"}});
    if (updates.at("results").items().size() != 4)
      throw std::runtime_error("expected four changed watch results");
    execute(setQueries, Json::Object{{"op", "setQueries"}, {"queries", desired}});
    auto local = run(client, Json::Object{{"op", "beginMutation"}, {"name", "probe"},
                                         {"timestamp", 2000 + i}});
    execute(mutation, Json::Object{{"op", "commitMutation"}, {"transaction", local.at("transaction")}});
  }
  client.close();
  Client reopened(argv[1], schema, "bench");
  auto pending = run(reopened, Json::Object{{"op", "pending"}});
  if (pending.items().size() != 1 || pending.items().front().at("id") != Json(170))
    throw std::runtime_error("incorrect mutation journal after reopening");
  auto rows = run(reopened, Json::Object{{"op", "query"}, {"ast", queries.at(2)}});
  if (rows.items().size() != 20 || Json::parse(rows.items().front().dump(true)).at("id") != Json("m1000"))
    throw std::runtime_error("incorrect message ordering after reopening");

  std::cout << "SQLite " << sqlite3_libversion() << ", 150 sessions / 1000 messages / 500 tasks, "
            << queries.size() << " watches, " << commit.samples.size() << " measured iterations\n"
            << std::fixed << std::setprecision(3);
  registerWatch.print("register watch (cold)");
  apply.print("beginPull/apply");
  commit.print("commitPull");
  publish.print("publish");
  setQueries.print("setQueries");
  mutation.print("commitMutation");
}
