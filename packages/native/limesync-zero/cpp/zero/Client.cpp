#include "Client.hpp"
#include "Json.hpp"

#include <sqlite3.h>
#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <functional>
#include <mutex>
#include <new>
#include <optional>
#include <set>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace limesync::zero {
namespace {
std::atomic<uint64_t> nextTransaction{0};

// prepared statements are reused across requests. a statement still stepping (an
// outer relationship query) is checked out, so a nested use of the same text
// prepares another.
class Statements {
 public:
  struct Lease {
    sqlite3_stmt* statement;
    // rehashing preserves cache entries until their owner is destroyed.
    std::vector<sqlite3_stmt*>* idle;
    std::string newKey;
  };

  explicit Statements(sqlite3* db) : db_(db) {}
  ~Statements() { clear(); }
  Statements(const Statements&) = delete;
  sqlite3* db() const { return db_; }
  Lease take(std::string_view sql) {
    auto found = idle_.find(sql);
    if (found != idle_.end() && !found->second.empty()) {
      sqlite3_stmt* stmt = found->second.back();
      found->second.pop_back();
      --count_;
      return {stmt, &found->second, {}};
    }
    Lease lease{nullptr, found != idle_.end() ? &found->second : nullptr, {}};
    if (!lease.idle) lease.newKey = sql;
    const auto& text = lease.idle ? found->first : lease.newKey;
    if (sqlite3_prepare_v3(db_, text.c_str(), -1, SQLITE_PREPARE_PERSISTENT, &lease.statement,
                          nullptr) != SQLITE_OK)
      throw std::runtime_error(sqlite3_errmsg(db_));
    return lease;
  }
  void give(Lease& lease) noexcept {
    sqlite3_stmt* stmt = lease.statement;
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
    if (count_ < 256) {
      try {
        auto& idle = lease.idle ? *lease.idle : idle_[std::move(lease.newKey)];
        idle.push_back(stmt);
        ++count_;
        return;
      } catch (const std::bad_alloc&) {
        // returning a statement from a destructor may run during unwinding.
      }
    }
    sqlite3_finalize(stmt);
  }
 private:
  void clear() {
    for (auto& [unused, stmts] : idle_)
      for (sqlite3_stmt* stmt : stmts) sqlite3_finalize(stmt);
    idle_.clear();
    count_ = 0;
  }

  struct SqlHash {
    using is_transparent = void;
    size_t operator()(std::string_view sql) const noexcept {
      return std::hash<std::string_view>{}(sql);
    }
  };
  sqlite3* db_;
  std::unordered_map<std::string, std::vector<sqlite3_stmt*>, SqlHash, std::equal_to<>> idle_;
  size_t count_ = 0;
};

class Statement {
 public:
  Statement(Statements& cache, std::string_view sql)
      : cache_(cache), db_(cache.db()), lease_(cache.take(sql)) {}
  ~Statement() { cache_.give(lease_); }
  Statement(const Statement&) = delete;
  sqlite3_stmt* get() const { return lease_.statement; }
  void bind(int index, const Json& v) {
    int code;
    if (v.null())
      code = sqlite3_bind_null(get(), index);
    else if (const auto* s = std::get_if<std::string>(&v.value))
      code =
          sqlite3_bind_text(get(), index, s->data(), static_cast<int>(s->size()), SQLITE_TRANSIENT);
    else if (const auto* n = std::get_if<double>(&v.value))
      code = sqlite3_bind_double(get(), index, *n);
    else if (const auto* b = std::get_if<bool>(&v.value))
      code = sqlite3_bind_int(get(), index, *b);
    else
      throw std::runtime_error("expected a scalar SQL parameter");
    if (code != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(db_));
  }
  // the text must outlive the step that reads it.
  void bindText(int index, std::string_view text) {
    if (sqlite3_bind_text(get(), index, text.data(), static_cast<int>(text.size()), SQLITE_STATIC) !=
        SQLITE_OK)
      throw std::runtime_error(sqlite3_errmsg(db_));
  }
  bool step() {
    int code = sqlite3_step(get());
    if (code == SQLITE_ROW) return true;
    if (code == SQLITE_NOMEM) throw std::bad_alloc();
    if (code != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db_));
    return false;
  }
  bool null(int col) const { return sqlite3_column_type(get(), col) == SQLITE_NULL; }
  std::string text(int col) const { return std::string(view(col)); }
  std::string_view view(int col) const {
    auto* value = sqlite3_column_text(get(), col);
    return value ? std::string_view(reinterpret_cast<const char*>(value),
                                    sqlite3_column_bytes(get(), col))
                 : std::string_view();
  }

 private:
  Statements& cache_;
  sqlite3* db_;
  Statements::Lease lease_;
};

void sql(Statements& db, const std::string& text, const std::vector<Json>& args = {}) {
  Statement statement(db, text);
  for (size_t i = 0; i < args.size(); ++i) statement.bind(static_cast<int>(i + 1), args[i]);
  while (statement.step()) {
  }
}

std::string identifier(const std::string& s) {
  std::string result = "\"";
  for (char c : s) {
    result += c;
    if (c == '"') result += '"';
  }
  return result + "\"";
}

std::string quoted(const std::string& s) {
  std::string result = "'";
  for (char c : s) {
    result += c;
    if (c == '\'') result += '\'';
  }
  return result + "'";
}

void like(sqlite3_context* context, int, sqlite3_value** args) try {
  if (sqlite3_value_type(args[0]) == SQLITE_NULL || sqlite3_value_type(args[1]) == SQLITE_NULL) {
    sqlite3_result_null(context);
    return;
  }
  if (sqlite3_value_type(args[0]) != SQLITE_TEXT || sqlite3_value_type(args[1]) != SQLITE_TEXT) {
    sqlite3_result_error(context, "LIKE requires strings", -1);
    return;
  }
  const auto* input = static_cast<const char16_t*>(sqlite3_value_text16(args[0]));
  const auto* pattern = static_cast<const char16_t*>(sqlite3_value_text16(args[1]));
  if (!input || !pattern) {
    sqlite3_result_error_nomem(context);
    return;
  }
  std::u16string_view text(input, sqlite3_value_bytes16(args[0]) / 2);
  std::u16string_view source(pattern, sqlite3_value_bytes16(args[1]) / 2);
  struct Token {
    char16_t value;
    bool literal;
  };
  std::vector<Token> tokens;
  for (size_t i = 0; i < source.size(); ++i) {
    if (source[i] == u'\\') {
      if (++i == source.size()) {
        sqlite3_result_error(context, "LIKE pattern must not end with escape character", -1);
        return;
      }
      tokens.push_back({source[i], true});
    } else
      tokens.push_back({source[i], source[i] != u'%' && source[i] != u'_'});
  }
  std::vector<bool> previous(text.size() + 1), next(text.size() + 1);
  previous[0] = true;
  for (const auto& token : tokens) {
    std::fill(next.begin(), next.end(), false);
    if (!token.literal && token.value == u'%') next[0] = previous[0];
    for (size_t i = 1; i <= text.size(); ++i)
      next[i] = !token.literal && token.value == u'%'
                    ? previous[i] || next[i - 1]
                    : previous[i - 1] &&
                          ((!token.literal && token.value == u'_') || text[i - 1] == token.value);
    previous.swap(next);
  }
  sqlite3_result_int(context, previous.back());
} catch (const std::bad_alloc&) {
  sqlite3_result_error_nomem(context);
}

int64_t integer(const Json& value, const std::string& label) {
  double number = value.number();
  if (number < 0 || number > 9007199254740991.0 || std::floor(number) != number)
    throw std::runtime_error(label + " must be a nonnegative safe integer");
  return static_cast<int64_t>(number);
}

struct Table {
  std::map<std::string, std::string> columns;
  std::map<std::string, std::string> serverColumns;
  std::set<std::string> optional;
  std::vector<std::string> primaryKey;
  // some server column name differs from its local name.
  bool renamed = false;
  // the sqlite table holding this table's rows, and its per-row statements.
  std::string rows;
  std::string upsert;
  std::string select;
  std::string remove;
  std::set<std::string> watchIndexes;
};

struct Compiled {
  std::string sql;
  std::vector<Json> args;
  std::vector<std::string_view> parentFields;
};

struct PreparedQuery {
  std::optional<Compiled> compiled;
  bool indexWatch = false;
  std::vector<PreparedQuery> related;
};

struct WatchDefinition {
  Json ast;
  Json format;
  std::set<std::string> tables;
  std::optional<std::string> pointKey;
  PreparedQuery plan;
  size_t observers = 0;
};

using WatchDefinitions = std::map<std::string, WatchDefinition>;

struct Watch {
  WatchDefinitions::iterator definition;
  std::string hash;
  bool published = false;
  size_t digest = 0;
  size_t size = 0;
  // the serialized rows of the last published list result, which a patch indexes
  std::shared_ptr<const std::vector<std::string>> rows;
  std::string resultType;
  std::string error;
};

// one query's answer: a list result keeps each serialized row so a watch can
// publish only the rows that changed since its last answer
struct Answer {
  bool succeeded = true;
  std::shared_ptr<const std::vector<std::string>> rows;
  std::string text;
  Json value;
  const std::vector<std::string>* comparedRows = nullptr;
  bool rowsMatch = false;

  bool matchesRows(const std::vector<std::string>& previous) {
    if (comparedRows != &previous) {
      comparedRows = &previous;
      rowsMatch = *rows == previous;
    }
    return rowsMatch;
  }

  Json row(size_t index) const {
    return Json::Serialized{(*rows)[index]};
  }
};

}  // namespace

struct Client::Impl {
  sqlite3* db = nullptr;
  std::unique_ptr<Statements> statements;
  std::mutex mutex;
  WatchDefinitions watchDefinitions;
  std::map<int64_t, Watch> watches;
  // a missing key set invalidates every watch on that table.
  std::unordered_map<std::string, std::optional<std::unordered_set<std::string>>> changedRows;
  bool allTablesChanged = false;
  // a publish answered from the open transaction; rolling it back must republish everything.
  bool publishedInTransaction = false;
  std::map<std::string, Table> tables;
  std::map<std::string, std::string> serverTables;
  std::string clientID;
  std::string transaction;
  std::string transactionKind;
  bool deleted = false;
  bool wal = false;
  bool cacheSync = false;

  Impl(const std::string& path, const Json& schema, const std::string& identity)
      : clientID(identity) {
    if (path.empty() || path.find('\0') != std::string::npos)
      throw std::runtime_error("database path is required");
    if (sqlite3_open_v2(path.c_str(), &db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                        nullptr) != SQLITE_OK) {
      std::string reason = sqlite3_errmsg(db);
      sqlite3_close_v2(db);
      db = nullptr;
      throw std::runtime_error(reason);
    }
    statements = std::make_unique<Statements>(db);
    try {
      sqlite3_busy_timeout(db, 5000);
      sqlite3_create_function_v2(db, "one_zero_like", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                                 nullptr, like, nullptr, nullptr, nullptr);
      for (const auto& [name, description] : schema.at("tables").fields()) {
        if (name.empty() || name.find('\0') != std::string::npos)
          throw std::runtime_error("invalid table name");
        Table table;
        for (const auto& [column, definition] : description.at("columns").fields()) {
          if (column.empty() || column.find('\0') != std::string::npos)
            throw std::runtime_error("invalid column name");
          std::string type = definition.at("type").string();
          if (type != "string" && type != "number" && type != "boolean" && type != "json")
            throw std::runtime_error("unsupported column type: " + type);
          table.columns.emplace(column, type);
          std::string serverColumn =
              definition.has("serverName") ? definition.at("serverName").string() : column;
          if (!table.serverColumns.emplace(serverColumn, column).second)
            throw std::runtime_error("duplicate server column name");
          if (serverColumn != column) table.renamed = true;
          if (definition.flag("optional")) table.optional.insert(column);
        }
        for (const Json& field : description.at("primaryKey").items()) {
          if (!table.columns.contains(field.string()))
            throw std::runtime_error("unknown primary key column");
          table.primaryKey.push_back(field.string());
        }
        if (table.primaryKey.empty()) throw std::runtime_error("table requires a primary key");
        std::string serverName =
            description.has("serverName") ? description.at("serverName").string() : name;
        if (!serverTables.emplace(serverName, name).second)
          throw std::runtime_error("duplicate server table name");
        table.rows = identifier("one_zero_rows:" + name);
        table.upsert = "INSERT INTO " + table.rows +
                       " VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value";
        table.select = "SELECT value FROM " + table.rows + " WHERE key=?";
        table.remove = "DELETE FROM " + table.rows + " WHERE key=?";
        tables.emplace(name, std::move(table));
      }
      {
        Statement mode(*statements, "PRAGMA journal_mode=WAL");
        wal = mode.step() && mode.view(0) == "wal";
      }
      sql(*statements, "PRAGMA synchronous=FULL");
      sql(*statements, "BEGIN IMMEDIATE");
      sql(*statements,
          "CREATE TABLE IF NOT EXISTS one_zero_meta(key TEXT PRIMARY "
          "KEY,value TEXT NOT NULL)");
      // each table's rows live in their own sqlite table, so writing a row maintains only that
      // table's indexes.
      for (const auto& [unused, definition] : tables)
        sql(*statements, "CREATE TABLE IF NOT EXISTS " + definition.rows +
                             "(key TEXT PRIMARY KEY,value TEXT NOT NULL)");
      sql(*statements,
          "CREATE TABLE IF NOT EXISTS one_zero_mutations(id INTEGER "
          "PRIMARY KEY,name TEXT NOT "
          "NULL,args TEXT NOT NULL,timestamp REAL NOT NULL)");
      // the server's row (NULL when absent) for each key a pending local write changed; every
      // other stored row is the server's row.
      sql(*statements,
          "CREATE TABLE IF NOT EXISTS one_zero_shadow(table_name TEXT NOT NULL,key TEXT NOT "
          "NULL,value TEXT,PRIMARY KEY(table_name,key))");
      std::string canonical = schema.dump();
      const Json format = meta("formatVersion");
      if (!format.null() && format != Json(1) && format != Json(2) && format != Json(3))
        throw std::runtime_error("unsupported native Zero store format");
      if (format == Json(1)) {
        // format 1 kept a full server copy in one_zero_base; keep only the rows it shadows.
        sql(*statements,
            "INSERT INTO one_zero_shadow SELECT b.table_name,b.key,b.value FROM one_zero_base b "
            "LEFT JOIN one_zero_view v ON v.table_name=b.table_name AND v.key=b.key WHERE "
            "v.value IS NOT b.value");
        sql(*statements,
            "INSERT INTO one_zero_shadow SELECT v.table_name,v.key,NULL FROM one_zero_view v "
            "WHERE NOT EXISTS (SELECT 1 FROM one_zero_base b WHERE b.table_name=v.table_name AND "
            "b.key=v.key)");
        sql(*statements, "DROP TABLE one_zero_base");
        sql(*statements, "DROP TABLE IF EXISTS one_zero_view_dirty");
      }
      if (format == Json(1) || format == Json(2)) {
        // formats 1 and 2 kept every table's rows in one_zero_view.
        for (const auto& [name, definition] : tables)
          sql(*statements,
              "INSERT INTO " + definition.rows +
                  " SELECT key,value FROM one_zero_view WHERE table_name=?",
              {name});
        sql(*statements, "DROP TABLE one_zero_view");
      }
      std::vector<std::string> staleTables;
      {
        Statement existingTables(*statements,
                                 "SELECT name FROM sqlite_master WHERE type='table' AND "
                                 "substr(name,1,14)='one_zero_rows:'");
        while (existingTables.step())
          if (!tables.contains(existingTables.text(0).substr(14)))
            staleTables.push_back(existingTables.text(0));
      }
      for (const auto& name : staleTables) sql(*statements, "DROP TABLE " + identifier(name));
      const Json existing = meta("schema");
      const bool sameSchema = existing == Json(canonical);
      if (clientID.empty()) {
        if (!meta("clientID").null())
          clientID = meta("clientID").string();
        else {
          Statement random(*statements, "SELECT lower(hex(randomblob(16)))");
          random.step();
          clientID = random.text(0);
        }
      }
      if (!meta("clientID").null() && meta("clientID") != Json(clientID))
        throw std::runtime_error("native Zero client identity differs from the persisted store");
      if (!existing.null() && !sameSchema) {
        clearRows();
        sql(*statements, "DELETE FROM one_zero_shadow");
        setMeta("cookie", Json());
        setMeta("queries", Json::Array{});
        setMeta("needsRebase", true);
        setMeta("completedQueries", Json::Array{});
        setMeta("queryErrors", Json::Object{});
      }
      std::map<std::string, std::string> indexes;
      const auto indexFields = [&](const std::string& name, const Json::Array& fields) {
        std::string expressions;
        for (const auto& field : fields) {
          if (!expressions.empty()) expressions += ',';
          expressions += column(name, field.string(), "");
        }
        if (expressions.empty()) return;
        indexes.emplace("one_zero_index:" + name + ":" + expressions,
                        table(name).rows + "(" + expressions + ")");
      };
      for (const auto& [name, definition] : tables) {
        Json::Array fields;
        for (const auto& field : definition.primaryKey) fields.emplace_back(field);
        indexFields(name, fields);
      }
      if (schema.has("relationships"))
        for (const auto& [unusedTable, relationships] : schema.at("relationships").fields())
          for (const auto& [unusedName, chain] : relationships.fields())
            for (const auto& part : chain.items())
              indexFields(part.at("destSchema").string(), part.at("destField").items());
      std::vector<std::string> staleIndexes;
      {
        Statement existingIndexes(*statements,
                                  "SELECT name FROM sqlite_master WHERE type='index' AND "
                                  "substr(name,1,15)='one_zero_index:'");
        while (existingIndexes.step())
          if (!indexes.contains(existingIndexes.text(0)))
            staleIndexes.push_back(existingIndexes.text(0));
      }
      {
        Statement watchIndexes(*statements,
                               "SELECT tbl_name,name FROM sqlite_master WHERE type='index' AND "
                               "substr(name,1,21)='one_zero_watch_index:'");
        while (watchIndexes.step()) {
          if (!sameSchema)
            staleIndexes.push_back(watchIndexes.text(1));
          else
            tables.at(std::string(watchIndexes.view(0).substr(14))).watchIndexes.insert(
                watchIndexes.text(1));
        }
      }
      for (const auto& name : staleIndexes) sql(*statements, "DROP INDEX " + quoted(name));
      for (const auto& [name, definition] : indexes)
        sql(*statements,
            "CREATE INDEX IF NOT EXISTS " + quoted(name) + " ON " + definition);
      setMeta("schema", canonical);
      setMeta("formatVersion", 3);
      setMeta("clientID", clientID);
      if (meta("nextMutationID").null()) setMeta("nextMutationID", 1);
      if (meta("lastMutationID").null()) setMeta("lastMutationID", 0);
      if (meta("cookie").null()) setMeta("cookie", Json());
      if (meta("queryVersion").null()) setMeta("queryVersion", 0);
      if (meta("queries").null()) setMeta("queries", Json::Array{});
      if (meta("completedQueries").null()) setMeta("completedQueries", Json::Array{});
      if (meta("queryErrors").null()) setMeta("queryErrors", Json::Object{});
      sql(*statements, "COMMIT");
    } catch (...) {
      statements.reset();
      sqlite3_close_v2(db);
      db = nullptr;
      throw;
    }
  }
  ~Impl() {
    statements.reset();
    if (db) sqlite3_close_v2(db);
  }

  Json meta(const std::string& name) {
    Statement statement(*statements, "SELECT value FROM one_zero_meta WHERE key=?");
    statement.bind(1, name);
    return statement.step() ? Json::parse(statement.text(0)) : Json();
  }
  void setMeta(const std::string& name, const Json& value) {
    sql(*statements,
        "INSERT INTO one_zero_meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET "
        "value=excluded.value",
        {name, value.dump()});
  }
  const Table& table(const std::string& name) const {
    auto found = tables.find(name);
    if (found == tables.end()) throw std::runtime_error("unknown table: " + name);
    return found->second;
  }
  std::string key(const std::string& name, const Json& row) const {
    std::string out = "[";
    for (const auto& field : table(name).primaryKey) {
      const Json& value = row.at(field);
      if (value.null() || value.object() || value.array())
        throw std::runtime_error("primary keys must be non-null scalars");
      if (out.size() > 1) out += ',';
      value.append(out);
    }
    return out + ']';
  }
  void validate(const std::string& name, Json& row, bool complete) const {
    const auto& definition = table(name);
    for (const auto& [column, value] : row.fields())
      if (!definition.columns.contains(column))
        throw std::runtime_error("unknown column: " + name + "." + column);
    auto& fields = std::get<Json::Object>(row.value);
    for (const auto& [column, type] : definition.columns) {
      auto found = fields.find(column);
      if (found == fields.end()) {
        if (!complete) continue;
        if (!definition.optional.contains(column))
          throw std::runtime_error("missing required column: " + column);
        found = fields.emplace(column, Json()).first;
      }
      const auto& value = found->second;
      if (value.null()) {
        if (!definition.optional.contains(column))
          throw std::runtime_error("required column is null: " + column);
      } else if ((type == "string" && !std::holds_alternative<std::string>(value.value)) ||
                 (type == "number" && !std::holds_alternative<double>(value.value)) ||
                 (type == "boolean" && !std::holds_alternative<bool>(value.value)))
        throw std::runtime_error("invalid column type: " + column);
    }
  }
  void store(const std::string& name, const std::string& rowKey, const Json& row) {
    const std::string text = row.dump();
    Statement statement(*statements, table(name).upsert);
    statement.bindText(1, rowKey);
    statement.bindText(2, text);
    while (statement.step()) {
    }
  }
  void write(const std::string& kind, const std::string& name, Json value) {
    std::string rowKey = key(name, value);
    if (kind == "delete") {
      shadow(name, rowKey);
      sql(*statements, table(name).remove, {rowKey});
      return;
    }
    if (kind != "insert" && kind != "upsert" && kind != "update")
      throw std::runtime_error("unsupported write kind: " + kind);
    {
      Statement existing(*statements, table(name).select);
      existing.bindText(1, rowKey);
      if (existing.step()) {
        if (kind == "insert") return;
        validate(name, value, false);
        Json old = Json::parse(existing.text(0));
        for (const auto& [column, field] : value.fields())
          std::get<Json::Object>(old.value).insert_or_assign(column, field);
        value = std::move(old);
      } else {
        if (kind == "update") return;
        validate(name, value, true);
      }
    }
    shadow(name, rowKey);
    store(name, rowKey, value);
  }
  void shadow(const std::string& name, const std::string& rowKey) {
    sql(*statements,
        "INSERT OR IGNORE INTO one_zero_shadow SELECT ?1,?2,(SELECT value FROM " + table(name).rows +
            " WHERE key=?2)",
        {name, rowKey});
    markChanged(name, rowKey);
  }
  void markChanged(const std::string& name, const std::string& rowKey) {
    if (allTablesChanged || watches.empty()) return;
    auto& keys = changedRows.try_emplace(name, std::in_place).first->second;
    // sqlite's json text extraction can truncate a string at an embedded nul.
    if (rowKey.find("\\u0000") != std::string::npos) keys.reset();
    if (keys) keys->insert(rowKey);
  }
  // server patches land in the stored rows unless a pending local write shadows the key.
  void serverStore(bool shadowed, const std::string& name, const std::string& rowKey,
                   const Json* row) {
    markChanged(name, rowKey);
    if (shadowed) {
      sql(*statements, "UPDATE one_zero_shadow SET value=? WHERE table_name=? AND key=?",
          {row ? Json(row->dump()) : Json(), name, rowKey});
      if (sqlite3_changes(db)) return;
    }
    if (row)
      store(name, rowKey, *row);
    else
      sql(*statements, table(name).remove, {rowKey});
  }
  void clearRows() {
    for (const auto& [unused, definition] : tables) sql(*statements, "DELETE FROM " + definition.rows);
  }
  std::pair<const std::string&, Json> mapServerRow(const std::string& serverName, Json row) {
    auto found = serverTables.find(serverName);
    if (found == serverTables.end())
      throw std::runtime_error("unknown server table: " + serverName);
    const auto& name = found->second;
    const auto& definition = table(name);
    row.fields();
    if (!definition.renamed) return {name, std::move(row)};
    Json::Object mapped;
    for (auto& [column, value] : std::get<Json::Object>(row.value)) {
      auto local = definition.serverColumns.find(column);
      mapped.emplace(local == definition.serverColumns.end() ? column : local->second,
                     std::move(value));
    }
    return {name, std::move(mapped)};
  }

  std::string column(const std::string& name, const std::string& field,
                     const std::string& alias) const {
    if (!table(name).columns.contains(field))
      throw std::runtime_error("unknown query column: " + field);
    std::string path = "$." + Json(field).dump();
    const auto value = alias.empty() ? "value" : alias + ".value";
    return "json_extract(" + value + "," + quoted(path) + ") COLLATE BINARY";
  }
  std::string operand(const Json& value, const std::string& name, const std::string& alias,
                      std::vector<Json>& args) {
    std::string type = value.at("type").string();
    if (type == "column") return column(name, value.at("name").string(), alias);
    if (type != "literal")
      throw std::runtime_error("query parameters must be bound before native execution");
    const auto& literal = value.at("value");
    if (literal.object() || literal.array())
      throw std::runtime_error("expected a scalar query literal");
    args.push_back(literal);
    return "?";
  }
  std::string condition(const Json& value, const std::string& name, const std::string& alias,
                        std::vector<Json>& args, unsigned depth,
                        std::set<std::string>* equalFields = nullptr) {
    if (depth > 32) throw std::runtime_error("query nesting exceeds 32 levels");
    std::string type = value.at("type").string();
    if (type == "and" || type == "or") {
      std::string result;
      for (const auto& item : value.at("conditions").items()) {
        if (!result.empty()) result += type == "and" ? " AND " : " OR ";
        result += condition(item, name, alias, args, depth + 1,
                            type == "and" ? equalFields : nullptr);
      }
      return "(" + (result.empty() ? (type == "and" ? "1" : "0") : result) + ")";
    }
    if (type == "correlatedSubquery") {
      std::string op = value.at("op").string();
      if (op != "EXISTS" && op != "NOT EXISTS")
        throw std::runtime_error("unsupported correlated predicate");
      const Json& related = value.at("related");
      std::string child = alias + "c";
      auto query = compile(related.at("subquery"), child, &related.at("correlation"), name, alias,
                           false, depth + 1);
      args.insert(args.end(), query.args.begin(), query.args.end());
      return op + "(" + query.sql + ")";
    }
    if (type != "simple") throw std::runtime_error("unsupported query condition: " + type);
    std::string op = value.at("op").string();
    const Json& right = value.at("right");
    const Json& lhs = value.at("left");
    std::string left = operand(lhs, name, alias, args);
    if (equalFields && (op == "=" || op == "IS" || op == "IN") &&
        lhs.at("type") == Json("column") && right.at("type") == Json("literal"))
      equalFields->insert(lhs.at("name").string());
    if (op == "IN" || op == "NOT IN") {
      if (right.at("type").string() != "literal")
        throw std::runtime_error("IN requires bound values");
      const auto& list = right.at("value").items();
      if (list.empty()) return "(" + left + " IS NOT NULL AND " + (op == "IN" ? "0" : "1") + ")";
      std::string placeholders;
      for (const auto& item : list) {
        if (!placeholders.empty()) placeholders += ',';
        placeholders += '?';
        args.push_back(item);
      }
      return "(" + left + " " + op + " (" + placeholders + "))";
    }
    static const std::set<std::string> operators = {
        "=", "!=", "IS", "IS NOT", "<", ">", "<=", ">=", "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE"};
    if (!operators.contains(op)) throw std::runtime_error("unsupported query operator: " + op);
    std::string rhs = operand(right, name, alias, args);
    if (op == "ILIKE" || op == "NOT ILIKE")
      throw std::runtime_error("native Unicode ILIKE is not yet supported");
    if (op == "LIKE" || op == "NOT LIKE") {
      return std::string(op.starts_with("NOT ") ? "NOT " : "") + "one_zero_like(" + left + "," +
             rhs + ")";
    }
    return "(" + left + " " + op + " " + rhs + ")";
  }

  Compiled compile(const Json& ast, const std::string& alias, const Json* correlation = nullptr,
                   const std::string& parentTable = "", const std::string& parentAlias = "",
                   bool bindParent = false, unsigned depth = 0, bool indexWatch = false) {
    if (depth > 32) throw std::runtime_error("query nesting exceeds 32 levels");
    std::string name = ast.at("table").string();
    const auto& definition = table(name);
    if (ast.has("schema"))
      throw std::runtime_error("schema-qualified native queries are not supported");
    Compiled out{"SELECT " + alias + ".value FROM " + definition.rows + " " + alias, {}, {}};
    bool filtered = false;
    const auto conjoin = [&] {
      out.sql += filtered ? " AND " : " WHERE ";
      filtered = true;
    };
    if (correlation) {
      const auto& parent = correlation->at("parentField").items();
      const auto& child = correlation->at("childField").items();
      if (parent.empty() || parent.size() != child.size())
        throw std::runtime_error("invalid relationship correlation");
      for (size_t i = 0; i < parent.size(); ++i) {
        conjoin();
        out.sql += column(name, child[i].string(), alias) + "=";
        if (bindParent) {
          out.sql += '?';
          out.parentFields.push_back(parent[i].string());
        } else
          out.sql += column(parentTable, parent[i].string(), parentAlias);
      }
    }
    std::set<std::string> equalFields;
    if (ast.has("where")) {
      conjoin();
      out.sql += condition(ast.at("where"), name, alias, out.args, depth + 1,
                           indexWatch ? &equalFields : nullptr);
    }
    std::vector<std::pair<std::string, std::string>> ordering;
    if (ast.has("orderBy")) {
      for (const auto& pair : ast.at("orderBy").items()) {
        if (pair.items().size() != 2) throw std::runtime_error("invalid ordering");
        std::string direction = pair.items()[1].string();
        if (direction != "asc" && direction != "desc")
          throw std::runtime_error("invalid ordering direction");
        ordering.emplace_back(pair.items()[0].string(), direction);
      }
    }
    for (const auto& field : definition.primaryKey) {
      if (std::none_of(ordering.begin(), ordering.end(),
                       [&](const auto& o) { return o.first == field; }))
        ordering.emplace_back(field, "asc");
    }
    if (ast.has("start")) {
      const Json& start = ast.at("start");
      std::string bounds;
      for (size_t i = 0; i < ordering.size(); ++i) {
        if (i) bounds += " OR ";
        bounds += '(';
        for (size_t j = 0; j < i; ++j) {
          bounds += column(name, ordering[j].first, alias) + " IS ? AND ";
          out.args.push_back(start.at("row").at(ordering[j].first));
        }
        std::string col = column(name, ordering[i].first, alias);
        const Json& bound = start.at("row").at(ordering[i].first);
        bool asc = ordering[i].second == "asc";
        if (bound.null())
          bounds += asc ? col + " IS NOT NULL" : "0";
        else {
          bounds += asc ? col + ">?" : "(" + col + "<? OR " + col + " IS NULL)";
          out.args.push_back(bound);
        }
        bounds += ')';
      }
      if (!start.flag("exclusive")) {
        bounds += " OR (";
        for (size_t i = 0; i < ordering.size(); ++i) {
          if (i) bounds += " AND ";
          bounds += column(name, ordering[i].first, alias) + " IS ?";
          out.args.push_back(start.at("row").at(ordering[i].first));
        }
        bounds += ')';
      }
      conjoin();
      out.sql += "(" + bounds + ")";
    }
    out.sql += " ORDER BY ";
    for (size_t i = 0; i < ordering.size(); ++i) {
      if (i) out.sql += ',';
      out.sql += column(name, ordering[i].first, alias) + " " + ordering[i].second;
    }
    if (ast.has("limit")) {
      out.sql += " LIMIT ?";
      out.args.push_back(static_cast<double>(integer(ast.at("limit"), "query limit")));
    }
    if (indexWatch) ensureWatchIndex(name, equalFields, ordering);
    return out;
  }

  void ensureWatchIndex(const std::string& name, const std::set<std::string>& equalFields,
                        const std::vector<std::pair<std::string, std::string>>& ordering) {
    auto& definition = tables.at(name);
    // values, limits and cursors share an index; bound write amplification per table.
    if (definition.watchIndexes.size() >= 8 || !sqlite3_get_autocommit(db)) return;
    std::vector<std::string> fields;
    std::string expressions;
    bool descending = false;
    auto append = [&](const std::string& field, const std::string& direction) {
      if (!expressions.empty()) expressions += ',';
      expressions += column(name, field, "");
      if (direction == "desc") {
        expressions += " DESC";
        descending = true;
      }
      fields.push_back(field);
    };
    for (const auto& field : equalFields)
      if (definition.columns.at(field) != "json") append(field, "asc");
    for (const auto& [field, direction] : ordering) {
      if (definition.columns.at(field) == "json") break;
      if (!equalFields.contains(field)) append(field, direction);
    }
    if (expressions.empty()) return;
    if (fields == definition.primaryKey && !descending) return;
    const auto index = "one_zero_watch_index:" + name + ":" + expressions;
    if (definition.watchIndexes.contains(index)) return;
    sql(*statements, "CREATE INDEX IF NOT EXISTS " + quoted(index) + " ON " +
                         definition.rows + "(" + expressions + ")");
    definition.watchIndexes.insert(index);
  }

  Json queryRelationship(const Json& related, const Json& childFormat, const Json& parent,
                         unsigned depth, PreparedQuery& plan) {
    const Json& child = related.at("subquery");
    if (!related.flag("hidden"))
      return query(child, childFormat, &related.at("correlation"), &parent, depth, &plan);
    std::string name = child.has("alias") ? child.at("alias").string() : child.at("table").string();
    Json nested;
    const auto& links = child.at("related").items();
    if (links.size() != 1 || links.front().flag("hidden"))
      throw std::runtime_error("expected a two-hop junction relationship");
    const Json& link = links.front();
    Json junction = child;
    auto& junctionFields = std::get<Json::Object>(junction.value);
    junctionFields.erase("related");
    Json::Array predicates;
    if (junction.has("where")) predicates.push_back(junction.at("where"));
    const auto& parentFields = related.at("correlation").at("parentField").items();
    const auto& junctionKeys = related.at("correlation").at("childField").items();
    if (parentFields.empty() || parentFields.size() != junctionKeys.size())
      throw std::runtime_error("invalid relationship correlation");
    for (size_t i = 0; i < parentFields.size(); ++i)
      predicates.emplace_back(Json::Object{
          {"type", "simple"}, {"op", "="},
          {"left", Json::Object{{"type", "column"}, {"name", junctionKeys[i]}}},
          {"right", Json::Object{{"type", "literal"},
                                 {"value", parent.at(parentFields[i].string())}}}});
    junctionFields.insert_or_assign(
        "where", Json::Object{{"type", "and"}, {"conditions", std::move(predicates)}});
    Json target = link.at("subquery");
    Json exists = Json::Object{
        {"type", "correlatedSubquery"}, {"op", "EXISTS"},
        {"related", Json::Object{
             {"correlation", Json::Object{
                  {"parentField", link.at("correlation").at("childField")},
                  {"childField", link.at("correlation").at("parentField")}}},
             {"subquery", std::move(junction)}}}};
    if (target.has("where"))
      exists = Json::Object{{"type", "and"},
                            {"conditions", Json::Array{target.at("where"), std::move(exists)}}};
    std::get<Json::Object>(target.value).insert_or_assign("where", std::move(exists));
    Json targetFormat = childFormat;
    if (childFormat.flag("singular")) {
      std::get<Json::Object>(target.value).insert_or_assign("limit", 2);
      std::get<Json::Object>(targetFormat.value).insert_or_assign("singular", false);
    }
    nested = query(target, targetFormat, nullptr, nullptr, depth);
    if (childFormat.flag("singular")) {
      auto& matches = std::get<Json::Array>(nested.value);
      if (matches.size() > 1)
        throw std::runtime_error("singular relationship has multiple rows: " + name);
      nested = matches.empty() ? Json() : Json(matches.front());
    }
    return nested;
  }

  void readQueryRows(const Json& ast, const Json* correlation, const Json* parent,
                     PreparedQuery& plan, auto&& read) {
    if (!plan.compiled)
      plan.compiled.emplace(compile(ast, "r", correlation, "", "", parent != nullptr, 0,
                                    plan.indexWatch));
    const auto& compiled = *plan.compiled;
    Statement statement(*statements, compiled.sql);
    int parameter = 1;
    if (parent)
      for (auto field : compiled.parentFields) statement.bind(parameter++, parent->at(field));
    for (const auto& arg : compiled.args) statement.bind(parameter++, arg);
    while (statement.step())
      if (!read(statement.view(0))) break;
  }

  Json query(const Json& ast, const Json& format, const Json* correlation = nullptr,
             const Json* parent = nullptr, unsigned depth = 0, PreparedQuery* prepared = nullptr) {
    if (depth > 32) throw std::runtime_error("relationship nesting exceeds 32 levels");
    PreparedQuery local;
    auto& plan = prepared ? *prepared : local;
    const bool hasRelated = ast.has("related");
    const bool singular = format.flag("singular");
    Json::Array rows;
    readQueryRows(ast, correlation, parent, plan, [&](std::string_view stored) {
      // stored rows already passed JSON validation; only parents need field access.
      Json row = hasRelated ? Json::parse(stored) : Json(Json::Serialized{std::string(stored)});
      if (hasRelated) {
        const auto& relationships = ast.at("related").items();
        if (plan.related.empty()) plan.related.resize(relationships.size());
        for (size_t i = 0; i < relationships.size(); ++i) {
          const auto& related = relationships[i];
          const Json& child = related.at("subquery");
          std::string name =
              child.has("alias") ? child.at("alias").string() : child.at("table").string();
          const Json& childFormat = format.get("relationships").get(name);
          Json nested = queryRelationship(related, childFormat, row, depth + 1, plan.related[i]);
          std::get<Json::Object>(row.value).insert_or_assign(name, std::move(nested));
        }
      }
      rows.push_back(std::move(row));
      return !singular;
    });
    if (singular) return rows.empty() ? Json() : std::move(rows.front());
    return rows;
  }

  void watchedTables(const Json& ast, std::set<std::string>& out, unsigned depth) {
    if (depth > 32) throw std::runtime_error("query nesting exceeds 32 levels");
    out.insert(ast.at("table").string());
    if (ast.has("related"))
      for (const Json& related : ast.at("related").items())
        watchedTables(related.at("subquery"), out, depth + 1);
    if (ast.has("where")) conditionTables(ast.at("where"), out, depth + 1);
  }
  void conditionTables(const Json& condition, std::set<std::string>& out, unsigned depth) {
    if (depth > 32) throw std::runtime_error("query nesting exceeds 32 levels");
    const std::string& type = condition.at("type").string();
    if (type == "and" || type == "or")
      for (const Json& item : condition.at("conditions").items())
        conditionTables(item, out, depth + 1);
    else if (type == "correlatedSubquery")
      watchedTables(condition.at("related").at("subquery"), out, depth + 1);
  }
  std::optional<std::string> pointWatchKey(const Json& ast) const {
    const auto& where = ast.get("where");
    if (ast.has("related") || where.get("type") != Json("simple") || where.get("op") != Json("="))
      return std::nullopt;
    const auto& left = where.get("left");
    const auto& right = where.get("right");
    if (left.get("type") != Json("column") || right.get("type") != Json("literal"))
      return std::nullopt;
    const auto* id = std::get_if<std::string>(&right.get("value").value);
    if (!id || id->find('\0') != std::string::npos) return std::nullopt;
    const auto found = tables.find(ast.at("table").string());
    if (found == tables.end()) return std::nullopt;
    const auto& primaryKey = found->second.primaryKey;
    if (primaryKey.size() != 1 || left.get("name") != Json(primaryKey.front()))
      return std::nullopt;
    return "[" + right.at("value").dump() + "]";
  }
  // answers the watches whose result, result type or error changed. one watch
  // when only is set: its first answer. a list result already published answers
  // with a patch: each row is the index of an identical row in the previous
  // answer or the new row itself, so the client parses and rebuilds only rows
  // that changed.
  Json publish(const int64_t* only) {
    const auto [first, end] =
        only ? watches.equal_range(*only) : std::pair{watches.begin(), watches.end()};
    const bool hashed = std::any_of(first, end, [](const auto& entry) {
      return !entry.second.hash.empty();
    });
    std::set<std::string> completed;
    Json errors = Json::Object{};
    if (hashed) {
      const Json completedQueries = meta("completedQueries");
      for (const Json& hash : completedQueries.items()) completed.insert(hash.string());
      errors = meta("queryErrors");
    }
    std::unordered_map<const WatchDefinition*, Answer> results;
    struct Publication {
      Watch& watch;
      const Answer* answer;
      int64_t id;
      std::string resultType;
      std::string error;
      size_t digest;
    };
    std::vector<Publication> changed;
    for (auto it = first; it != end; ++it) {
      auto& [id, watch] = *it;
      auto& definition = watch.definition->second;
      const bool failed = !watch.hash.empty() && errors.has(watch.hash);
      std::string error = failed ? errors.at(watch.hash).string() : "";
      std::string resultType = failed                            ? "error"
                               : completed.contains(watch.hash) ? "complete"
                                                                : "unknown";
      const bool dirty = allTablesChanged || std::any_of(
          definition.tables.begin(), definition.tables.end(), [&](const std::string& table) {
            const auto found = changedRows.find(table);
            if (found == changedRows.end()) return false;
            const auto& keys = found->second;
            return !definition.pointKey || !keys || keys->contains(*definition.pointKey);
          });
      const bool sameState = resultType == watch.resultType && error == watch.error;
      if (watch.published && !dirty) {
        if (sameState) continue;
        if (watch.rows) {
          changed.push_back({watch, nullptr, id, std::move(resultType), std::move(error), 0});
          continue;
        }
      }
      auto found = results.find(&definition);
      if (found == results.end()) {
        Answer answer;
        try {
          if (!definition.ast.has("related") && !definition.format.flag("singular")) {
            auto rows = std::make_shared<std::vector<std::string>>();
            readQueryRows(definition.ast, nullptr, nullptr, definition.plan, [&](std::string_view stored) {
              rows->emplace_back(stored);
              return true;
            });
            answer.rows = std::move(rows);
          } else {
            Json value = query(definition.ast, definition.format, nullptr, nullptr, 0, &definition.plan);
            if (value.array()) {
              auto rows = std::make_shared<std::vector<std::string>>();
              rows->reserve(value.items().size());
              for (const Json& row : value.items()) rows->push_back(row.dump(true));
              answer.rows = std::move(rows);
            } else {
              answer.text = value.dump();
              answer.value = std::move(value);
            }
          }
        } catch (const std::bad_alloc&) {
          throw;
        } catch (const std::exception& failure) {
          answer = {false, {}, failure.what(), {}};
        }
        found = results.emplace(&definition, std::move(answer)).first;
      }
      Answer& answer = found->second;
      size_t digest = 0;
      if (answer.rows) {
        if (watch.published && watch.rows && sameState && answer.matchesRows(*watch.rows))
          continue;
      } else if (answer.succeeded) {
        digest = std::hash<std::string_view>{}(answer.text);
        if (watch.published && !watch.rows && digest == watch.digest &&
            answer.text.size() == watch.size && sameState)
          continue;
      }
      changed.push_back({watch, &answer, id, std::move(resultType), std::move(error), digest});
    }

    Json::Array output;
    output.reserve(changed.size());
    for (const auto& [watch, answer, id, resultType, error, digest] : changed) {
      Json::Object entry{{"id", static_cast<double>(id)}};
      if (answer && !answer->succeeded) {
        entry.emplace("failure", answer->text);
      } else {
        entry.emplace("resultType", resultType);
        if (resultType == "error") entry.emplace("error", error);
        if (answer && !answer->rows) {
          entry.emplace("value", answer->value);
        } else if (watch.published && watch.rows) {
          const auto& previous = *watch.rows;
          const auto& current = answer ? *answer->rows : previous;
          Json::Array patch(current.size());
          size_t aligned = 0;
          // relationship projections can overwrite a primary key and produce duplicate rows.
          if (!watch.definition->second.ast.has("related")) {
            const auto shared = std::min(previous.size(), current.size());
            for (size_t i = 0; i < shared; ++i) {
              if (!answer || previous[i] == current[i]) {
                patch[i] = static_cast<double>(i);
                ++aligned;
              }
            }
          }
          if (aligned != current.size()) {
            std::vector<size_t> indices(std::bit_ceil((previous.size() - aligned) * 2 + 1));
            const size_t mask = indices.size() - 1;
            const auto findRow = [&](std::string_view row) -> size_t& {
              size_t bucket = std::hash<std::string_view>{}(row) & mask;
              while (indices[bucket] && previous[indices[bucket] - 1] != row)
                bucket = (bucket + 1) & mask;
              return indices[bucket];
            };
            for (size_t i = 0; i < previous.size(); ++i) {
              if (i < patch.size() && !patch[i].null()) continue;
              auto& index = findRow(previous[i]);
              if (!index) index = i + 1;
            }
            for (size_t i = 0; i < current.size(); ++i) {
              if (!patch[i].null()) continue;
              const auto same = findRow(current[i]);
              patch[i] = same ? Json(static_cast<double>(same - 1)) : answer->row(i);
            }
          }
          entry.emplace("patch", std::move(patch));
        } else {
          Json::Array rows;
          rows.reserve(answer->rows->size());
          for (size_t i = 0; i < answer->rows->size(); ++i) rows.push_back(answer->row(i));
          entry.emplace("value", std::move(rows));
        }
      }
      output.emplace_back(std::move(entry));
    }

    Json::Object result;
    result.emplace("results", std::move(output));
    for (auto& [watch, answer, id, resultType, error, digest] : changed) {
      if (answer) {
        watch.published = answer->succeeded;
        watch.rows = answer->rows;
        watch.digest = digest;
        watch.size = answer->text.size();
      }
      watch.resultType = std::move(resultType);
      watch.error = std::move(error);
    }
    if (!only) {
      changedRows.clear();
      allTablesChanged = false;
      publishedInTransaction = !transaction.empty();
    }
    return result;
  }
  Json pending() {
    Statement statement(*statements, "SELECT id,name,args,timestamp FROM one_zero_mutations ORDER BY id");
    Json::Array result;
    while (statement.step()) {
      Json::Object mutation{{"id", static_cast<double>(sqlite3_column_int64(statement.get(), 0))},
                            {"name", statement.text(1)},
                            {"timestamp", sqlite3_column_double(statement.get(), 3)},
                            {"clientID", clientID}};
      // the journal stores args as dumped JSON; empty text means omitted args.
      auto args = statement.text(2);
      if (!args.empty()) mutation.emplace("args", Json::parse(args));
      result.emplace_back(std::move(mutation));
    }
    return result;
  }
  Json state(const Json& knownQueryVersion = Json()) {
    const Json queryVersion = meta("queryVersion");
    Json::Object result{{"clientID", clientID},
                        {"clientGroupID", clientID},
                        {"cookie", meta("cookie")},
                        {"nextMutationID", meta("nextMutationID")},
                        {"lastMutationID", meta("lastMutationID")},
                        {"queryVersion", queryVersion},
                        {"recoveryRequired", meta("recoveryRequired")},
                        {"completedQueries", meta("completedQueries")},
                        {"queryErrors", meta("queryErrors")}};
    if (knownQueryVersion != queryVersion) result.emplace("queries", meta("queries"));
    return result;
  }
  void setCacheSync(bool enabled) {
    enabled = enabled && wal;
    if (cacheSync == enabled) return;
    sql(*statements, enabled ? "PRAGMA synchronous=NORMAL" : "PRAGMA synchronous=FULL");
    cacheSync = enabled;
  }
  void begin(const std::string& kind) {
    if (!transaction.empty()) throw std::runtime_error("a native transaction is already active");
    // cache commits can replay after a power loss; local mutation ids and writes cannot.
    setCacheSync(kind == "pull" || kind == "queries");
    auto id = std::to_string(++nextTransaction);
    auto transactionType = kind;
    sql(*statements, "BEGIN IMMEDIATE");
    transaction = std::move(id);
    transactionKind = std::move(transactionType);
  }
  void guard(const Json& request) {
    if (transaction.empty()) {
      if (request.has("transaction")) throw std::runtime_error("native transaction has ended");
    } else if (!request.has("transaction") || request.at("transaction").string() != transaction)
      throw std::runtime_error("native transaction does not belong to this operation");
  }
  void rollback() {
    transaction.clear();
    transactionKind.clear();
    if (publishedInTransaction) allTablesChanged = true;
    publishedInTransaction = false;
    if (!sqlite3_get_autocommit(db)) sql(*statements, "ROLLBACK");
  }
  void commit() {
    sql(*statements, "COMMIT");
    transaction.clear();
    transactionKind.clear();
    publishedInTransaction = false;
  }
  Json execute(Json& request) {
    if (!db || deleted) throw std::runtime_error("native Zero client is closed");
    std::string op = request.at("op").string();
    if (op == "beginMutation") {
      if (!meta("recoveryRequired").null())
        throw std::runtime_error("CLIENT_STATE_NOT_FOUND: pending writes require reconciliation");
      std::string name = request.at("name").string();
      if (name.empty()) throw std::runtime_error("mutation name is required");
      double timestamp = request.at("timestamp").number();
      begin("mutation");
      try {
        int64_t id = integer(meta("nextMutationID"), "mutation id");
        if (id >= 9007199254740991LL) throw std::runtime_error("mutation id exhausted");
        Json::Object mutation{{"id", static_cast<double>(id)},
                              {"name", name},
                              {"timestamp", timestamp},
                              {"clientID", clientID}};
        // empty text cannot be JSON; it preserves omitted arguments in existing journals.
        if (request.has("args")) mutation.emplace("args", request.at("args"));
        sql(*statements, "INSERT INTO one_zero_mutations VALUES(?,?,?,?)",
            {static_cast<double>(id), name,
             request.has("args") ? request.at("args").dump() : "", timestamp});
        setMeta("nextMutationID", static_cast<double>(id + 1));
        return Json::Object{{"transaction", transaction}, {"mutation", mutation}};
      } catch (...) {
        rollback();
        throw;
      }
    }
    if (op == "beginPull") {
      const auto& input = request.at("response");
      std::optional<Json> parsed;
      if (const auto* body = std::get_if<std::string>(&input.value))
        parsed.emplace(Json::parsePullResponse(*body));
      Json& response = parsed ? *parsed : request.at("response");
      if (response.has("error"))
        throw std::runtime_error("server rejected pull: " + response.at("error").dump());
      const Json& cookie = response.at("cookie");
      if (request.has("baseCookie") && request.at("baseCookie") != meta("cookie"))
        throw std::runtime_error("pull base cookie does not match persisted state");
      if (request.has("queryVersion") && request.at("queryVersion") != meta("queryVersion"))
        throw std::runtime_error("pull query version does not match persisted state");
      if (response.has("baseCookie") && response.at("baseCookie") != meta("cookie"))
        throw std::runtime_error("pull base cookie does not match persisted state");
      if (response.get("lastMutationIDChanges").has(clientID) &&
          integer(response.at("lastMutationIDChanges").at(clientID), "mutation acknowledgement") >=
              integer(meta("nextMutationID"), "next mutation id")) {
        setCacheSync(false);
        setMeta("recoveryRequired", response.at("lastMutationIDChanges").at(clientID));
        throw std::runtime_error(
            "CLIENT_STATE_NOT_FOUND: server "
            "acknowledgement exceeds local journal");
      }
      if (!meta("recoveryRequired").null())
        throw std::runtime_error("CLIENT_STATE_NOT_FOUND: pending writes require reconciliation");
      begin("pull");
      try {
        int64_t previousAck = integer(meta("lastMutationID"), "mutation acknowledgement");
        int64_t ack = previousAck;
        if (response.get("lastMutationIDChanges").has(clientID)) {
          int64_t incoming = integer(response.at("lastMutationIDChanges").at(clientID),
                                     "mutation acknowledgement");
          if (incoming < ack || incoming >= integer(meta("nextMutationID"), "next mutation id"))
            throw std::runtime_error("mutation acknowledgement is outside the client journal");
          ack = incoming;
        }
        sql(*statements, "DELETE FROM one_zero_mutations WHERE id<=?", {static_cast<double>(ack)});
        bool shadowed;
        {
          Statement any(*statements, "SELECT 1 FROM one_zero_shadow LIMIT 1");
          shadowed = any.step();
        }
        bool changed = ack != previousAck ||
                       (response.has("rowsPatch") && response.at("rowsPatch").hasItems()) ||
                       meta("needsRebase") == Json(true);
        const Json previousCompleted = meta("completedQueries");
        const Json previousErrors = meta("queryErrors");
        Json::Object errors = previousErrors.fields();
        std::set<std::string> completed;
        for (const auto& hash : previousCompleted.items()) completed.insert(hash.string());
        if (response.has("rowsPatch")) {
          response.at("rowsPatch").consumeItems([&](Json&& patch) {
            std::string kind = patch.at("op").string();
            if (kind == "clear") {
              clearRows();
              sql(*statements, "DELETE FROM one_zero_shadow");
              shadowed = false;
              allTablesChanged = true;
              completed.clear();
              errors.clear();
            } else if (kind == "put" || kind == "del") {
              if (!serverTables.contains(patch.at("tableName").string())) return;
              auto [name, row] = mapServerRow(patch.at("tableName").string(),
                                              std::move(patch.at(kind == "put" ? "value" : "id")));
              serverStore(shadowed, name, key(name, row), kind == "put" ? &row : nullptr);
            } else if (kind == "update") {
              if (!serverTables.contains(patch.at("tableName").string())) return;
              auto [name, id] = mapServerRow(patch.at("tableName").string(), std::move(patch.at("id")));
              Statement existing(*statements,
                                 "SELECT value FROM one_zero_shadow WHERE table_name=?1 AND "
                                 "key=?2 UNION ALL SELECT value FROM " + table(name).rows +
                                     " WHERE key=?2 AND NOT EXISTS (SELECT 1 FROM "
                                     "one_zero_shadow WHERE table_name=?1 AND key=?2)");
              existing.bind(1, name);
              existing.bind(2, key(name, id));
              if (!existing.step() || existing.null(0))
                throw std::runtime_error("row patch updates an absent row");
              Json row = Json::parse(existing.text(0));
              if (patch.has("merge")) {
                auto [unused, merge] =
                    mapServerRow(patch.at("tableName").string(), std::move(patch.at("merge")));
                for (const auto& [column, value] : merge.fields())
                  std::get<Json::Object>(row.value).insert_or_assign(column, value);
              }
              if (patch.has("constrain") && !patch.at("constrain").items().empty()) {
                std::set<std::string> keep;
                for (const auto& field : patch.at("constrain").items()) {
                  auto mapped = table(name).serverColumns.find(field.string());
                  keep.insert(mapped == table(name).serverColumns.end() ? field.string()
                                                                        : mapped->second);
                }
                auto& fields = std::get<Json::Object>(row.value);
                for (auto it = fields.begin(); it != fields.end();) {
                  if (!keep.contains(it->first))
                    it = fields.erase(it);
                  else
                    ++it;
                }
              }
              serverStore(shadowed, name, key(name, row), &row);
            } else
              throw std::runtime_error("unsupported row patch: " + kind);
          });
        }
        // orez answers gotQueries {version,patch}; other servers send the patch.
        const Json& gotQueries = response.get("gotQueries");
        if (!gotQueries.null()) integer(gotQueries.at("version"), "completed query version");
        const Json& gotPatch =
            gotQueries.null() ? response.get("gotQueriesPatch") : gotQueries.at("patch");
        if (!gotPatch.null()) {
          for (const auto& patch : gotPatch.items()) {
            const std::string kind = patch.at("op").string();
            if (kind == "clear") {
              completed.clear();
              errors.clear();
            } else if (kind == "put") {
              completed.insert(patch.at("hash").string());
              errors.erase(patch.at("hash").string());
            } else if (kind == "del") {
              completed.erase(patch.at("hash").string());
              errors.erase(patch.at("hash").string());
            } else
              throw std::runtime_error("unsupported completed query patch: " + kind);
          }
        }
        if (response.has("queryErrors")) {
          for (const auto& error : response.at("queryErrors").items()) {
            const std::string hash = error.at("hash").string();
            completed.erase(hash);
            errors.insert_or_assign(hash, error.at("message").string());
          }
        }
        Json::Array completedValues;
        for (const auto& hash : completed) completedValues.emplace_back(hash);
        Json nextCompleted(std::move(completedValues));
        setMeta("completedQueries", nextCompleted);
        Json nextErrors(std::move(errors));
        setMeta("queryErrors", nextErrors);
        if (changed && shadowed) {
          // restore the server rows; the caller replays pending writes over them.
          std::vector<std::string> restored;
          {
            Statement names(*statements, "SELECT DISTINCT table_name FROM one_zero_shadow");
            while (names.step()) restored.push_back(names.text(0));
          }
          for (const auto& name : restored) {
            changedRows[name].reset();
            const auto& rows = table(name).rows;
            sql(*statements,
                "DELETE FROM " + rows +
                    " WHERE key IN (SELECT key FROM one_zero_shadow WHERE table_name=?)",
                {name});
            sql(*statements,
                "INSERT INTO " + rows +
                    " SELECT key,value FROM one_zero_shadow WHERE table_name=? AND value IS NOT NULL",
                {name});
          }
          sql(*statements, "DELETE FROM one_zero_shadow");
        }
        setMeta("cookie", cookie);
        setMeta("lastMutationID", static_cast<double>(ack));
        setMeta("needsRebase", false);
        return Json::Object{{"transaction", transaction},
                            {"lastMutationID", static_cast<double>(ack)},
                            {"pending", changed ? pending() : Json(Json::Array{})},
                            {"changed", changed || nextCompleted != previousCompleted ||
                                            nextErrors != previousErrors}};
      } catch (...) {
        rollback();
        throw;
      }
    }
    guard(request);
    if (op == "delete") {
      begin("delete");
      try {
        bool recovery = !meta("recoveryRequired").null() ||
                        (request.has("dropPending") && request.at("dropPending").boolean());
        if (recovery) {
          sql(*statements, "DELETE FROM one_zero_mutations");
        } else {
          Statement pending(*statements, "SELECT 1 FROM one_zero_mutations LIMIT 1");
          if (pending.step())
            throw std::runtime_error("native Zero cannot delete a store with pending mutations");
        }
        clearRows();
        sql(*statements, "DELETE FROM one_zero_shadow");
        sql(*statements, "DELETE FROM one_zero_meta");
        commit();
        allTablesChanged = true;
        deleted = true;
        return Json();
      } catch (...) {
        rollback();
        throw;
      }
    }
    if (op == "setQueries") {
      if (request.has("queries")) {
        std::set<std::string> hashes;
        for (const auto& desired : request.at("queries").items()) {
          auto hash = desired.at("hash").string();
          if (hash.empty() || !hashes.insert(hash).second)
            throw std::runtime_error("desired queries require distinct nonempty hashes");
          if (desired.has("ast") == desired.has("name"))
            throw std::runtime_error("desired query requires either an AST or a name");
          if (desired.has("name")) {
            desired.at("name").string();
            desired.at("args").items();
          } else
            compile(desired.at("ast"), "r");
        }
      }
      begin("queries");
      try {
        auto version = integer(meta("queryVersion"), "desired query version");
        if (version >= 9007199254740991LL)
          throw std::runtime_error("desired query version exhausted");
        setMeta("queryVersion", static_cast<double>(version + 1));
        if (request.has("queries")) setMeta("queries", request.at("queries"));
        commit();
        return state();
      } catch (...) {
        rollback();
        throw;
      }
    }
    if (op == "query") {
      return query(request.at("ast"), request.get("format"));
    }
    if (op == "watch") {
      int64_t id = integer(request.at("id"), "watch id");
      const auto& ast = request.at("ast");
      const auto& format = request.get("format");
      std::string hash = request.has("hash") ? request.at("hash").string() : "";
      std::string key = "{\"ast\":";
      ast.append(key);
      key += ",\"format\":";
      format.append(key);
      key += '}';
      auto definition = watchDefinitions.find(key);
      if (definition == watchDefinitions.end()) {
        WatchDefinition value{ast, format};
        watchedTables(value.ast, value.tables, 0);
        value.pointKey = pointWatchKey(value.ast);
        value.plan.indexWatch = !value.pointKey;
        definition = watchDefinitions.emplace(std::move(key), std::move(value)).first;
      }
      try {
        if (!watches.emplace(id, Watch{definition, std::move(hash)}).second)
          throw std::runtime_error("watch id is already registered");
      } catch (...) {
        if (!definition->second.observers) watchDefinitions.erase(definition);
        throw;
      }
      ++definition->second.observers;
      return publish(&id);
    }
    if (op == "unwatch") {
      const auto watch = watches.find(integer(request.at("id"), "watch id"));
      if (watch != watches.end()) {
        const auto definition = watch->second.definition;
        watches.erase(watch);
        if (--definition->second.observers == 0) watchDefinitions.erase(definition);
      }
      return Json();
    }
    if (op == "publish") return publish(nullptr);
    if (op == "state") return state(request.get("queryVersion"));
    if (op == "pending") return pending();
    if (op == "rollback") {
      rollback();
      return Json();
    }
    if (op == "write") {
      if (transaction.empty())
        throw std::runtime_error("writes require an active native transaction");
      write(request.at("kind").string(), request.at("table").string(), request.at("value"));
      return Json();
    }
    if (op == "commitMutation" || op == "commitPull") {
      if (transaction.empty() || transactionKind != (op == "commitMutation" ? "mutation" : "pull"))
        throw std::runtime_error("wrong native transaction kind");
      commit();
      return state(request.get("queryVersion"));
    }
    throw std::runtime_error("unsupported native Zero operation: " + op);
  }
};

Client::Client(const std::string& path, const Json& schema,
               const std::string& clientID)
    : impl_(std::make_unique<Impl>(path, schema, clientID)) {}
Client::~Client() = default;
Json Client::executeValue(Json& request) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (!impl_->db) throw std::runtime_error("native Zero client is closed");
  return impl_->execute(request);
}
void Client::close() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (!impl_->db) return;
  impl_->rollback();
  impl_->statements.reset();
  sqlite3* db = impl_->db;
  impl_->db = nullptr;
  int result = sqlite3_close_v2(db);
  if (result != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(db));
}

}  // namespace limesync::zero
