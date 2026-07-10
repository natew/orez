//! Executable M0 proof of the native host's rusqlite transaction behavior.

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, Result, params};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    fn database() -> Connection {
        let db = Connection::open_in_memory().expect("open SQLite");
        db.execute_batch(
            "CREATE TABLE state (singleton INTEGER PRIMARY KEY, lmid INTEGER NOT NULL);
             CREATE TABLE accounts (id TEXT PRIMARY KEY, balance REAL NOT NULL);
             CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount REAL NOT NULL);
             INSERT INTO state VALUES (1, 0);
             INSERT INTO accounts VALUES ('primary', 100.0);",
        )
        .expect("create probe schema");
        db
    }

    fn snapshot(db: &Connection) -> (i64, f64, i64) {
        db.query_row(
            "SELECT
               (SELECT lmid FROM state WHERE singleton = 1),
               (SELECT balance FROM accounts WHERE id = 'primary'),
               (SELECT COUNT(*) FROM ledger)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read snapshot")
    }

    #[test]
    fn read_then_write_and_multi_table_commit_atomically() {
        let mut db = database();
        let tx = db.transaction().expect("begin host transaction");
        let balance: f64 = tx
            .query_row(
                "SELECT balance FROM accounts WHERE id = ?",
                ["primary"],
                |row| row.get(0),
            )
            .expect("read balance");
        tx.execute(
            "UPDATE accounts SET balance = ? WHERE id = ?",
            params![balance + 10.0, "primary"],
        )
        .expect("update account");
        tx.execute("INSERT INTO ledger VALUES (?, ?)", params![1, 10.0])
            .expect("insert ledger");
        tx.execute("UPDATE state SET lmid = lmid + 1 WHERE singleton = 1", [])
            .expect("advance LMID");
        tx.commit().expect("commit host transaction");
        assert_eq!(snapshot(&db), (1, 110.0, 1));
    }

    #[test]
    fn returned_application_error_rolls_back_every_table_and_lmid() {
        let mut db = database();
        let before = snapshot(&db);
        let result: Result<()> = (|| {
            let tx = db.transaction()?;
            tx.execute(
                "UPDATE accounts SET balance = balance + 777 WHERE id = 'primary'",
                [],
            )?;
            tx.execute("INSERT INTO ledger VALUES (?, ?)", params![1, 777.0])?;
            tx.execute("UPDATE state SET lmid = lmid + 1 WHERE singleton = 1", [])?;
            Err(rusqlite::Error::InvalidQuery)
        })();
        assert!(result.is_err());
        assert_eq!(snapshot(&db), before);
    }

    #[test]
    fn panic_drops_transaction_and_rolls_back_every_table_and_lmid() {
        let mut db = database();
        let before = snapshot(&db);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let tx = db.transaction().expect("begin host transaction");
            tx.execute(
                "UPDATE accounts SET balance = balance - 99 WHERE id = 'primary'",
                [],
            )
            .expect("update account");
            tx.execute("INSERT INTO ledger VALUES (?, ?)", params![1, -99.0])
                .expect("insert ledger");
            tx.execute("UPDATE state SET lmid = lmid + 1 WHERE singleton = 1", [])
                .expect("advance LMID");
            panic!("intentional native probe panic")
        }));
        assert!(panic.is_err());
        assert_eq!(snapshot(&db), before);
    }
}
