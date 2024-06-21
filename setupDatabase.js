const oracledb = require("oracledb");

async function setupDatabase(connection) {
  try {
    await connection.execute(
      `BEGIN
      execute immediate 'drop table users CASCADE CONSTRAINTS';
      execute immediate 'drop table accounts CASCADE CONSTRAINTS';
      execute immediate 'drop table transactions CASCADE CONSTRAINTS';
      exception when others then if sqlcode <> -942 then raise; end if;
      END;`
    );

    // TABLES
    await connection.execute(
      `CREATE TABLE users (
        id NUMBER GENERATED ALWAYS AS IDENTITY,
        name VARCHAR2(256),
        email VARCHAR2(512),
        creation_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        accounts NUMBER DEFAULT 0,
        PRIMARY KEY (id)
      )`
    );

    await connection.execute(
      `CREATE TABLE accounts (
        id NUMBER GENERATED ALWAYS AS IDENTITY,
        name VARCHAR2(256),
        amount NUMBER DEFAULT 0,
        user_id NUMBER,
        transactions NUMBER DEFAULT 0,
        CONSTRAINT fk_user
        FOREIGN KEY (user_id)
        REFERENCES users (id),
        creation_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
      )`
    );

    await connection.execute(
      `CREATE TABLE transactions (
        id NUMBER GENERATED ALWAYS AS IDENTITY,
        name VARCHAR2(256),
        amount NUMBER,
        type NUMBER CHECK (type IN (0, 1)),
        account_id NUMBER,
        CONSTRAINT fk_account
        FOREIGN KEY (account_id)
        REFERENCES accounts (id),
        creation_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
      )`
    );

    // PROCEDURES
    await connection.execute(
      `CREATE OR REPLACE PROCEDURE format_transaction_name (
        p_transaction_type IN NUMBER,
        p_transaction_name IN VARCHAR2,
        p_formatted_name OUT VARCHAR2
      ) AS
      BEGIN
        p_formatted_name := 'T' || p_transaction_type || '-' || UPPER(p_transaction_name);
      END;`
    );

    await connection.execute(
      `CREATE OR REPLACE PROCEDURE insert_user (
        p_user_name IN users.name%TYPE,
        p_user_email IN users.email%TYPE,
        p_user_id OUT users.id%TYPE
      ) AS
      BEGIN
        INSERT INTO users (name, email, accounts)
        VALUES (p_user_name, p_user_email, 0)
        RETURNING id INTO p_user_id;
      END;`
    );

    await connection.execute(
      `CREATE OR REPLACE PROCEDURE insert_account (
        p_account_name IN accounts.name%TYPE,
        p_account_amount IN accounts.amount%TYPE,
        p_user_id IN accounts.user_id%TYPE
      ) AS
      BEGIN
        INSERT INTO accounts (name, amount, user_id, transactions)
        VALUES (p_account_name, p_account_amount, p_user_id, 0);
        UPDATE users SET accounts = accounts + 1 WHERE id = p_user_id;
      END;`
    );

    await connection.execute(
      `CREATE OR REPLACE PROCEDURE insert_transaction (
        p_transaction_name IN transactions.name%TYPE,
        p_transaction_amount IN transactions.amount%TYPE,
        p_transaction_type IN transactions.type%TYPE,
        p_account_id IN transactions.account_id%TYPE
      ) AS
      v_formatted_name VARCHAR2(256);
      BEGIN
        format_transaction_name(p_transaction_type, p_transaction_name, v_formatted_name);
        INSERT INTO transactions (name, amount, type, account_id)
        VALUES (v_formatted_name, p_transaction_amount, p_transaction_type, p_account_id);
        
        UPDATE accounts SET transactions = transactions + 1 WHERE id = p_account_id;
      END;`
    );

    await connection.execute(
      `CREATE OR REPLACE PROCEDURE export_transactions_to_csv(p_account_id IN NUMBER) IS
        v_file UTL_FILE.FILE_TYPE;
        v_line VARCHAR2(32767);
      BEGIN
        v_file := UTL_FILE.FOPEN('EXPORT_DIR', 'transactions.csv', 'W');
        UTL_FILE.PUT_LINE(v_file, 'ID,NAME,AMOUNT,TYPE,ACCOUNT_ID,CREATION_TS');

        FOR rec IN (SELECT id, name, amount, type, account_id, creation_ts 
                    FROM transactions WHERE account_id = p_account_id) LOOP
          v_line := rec.id || ',' || rec.name || ',' || rec.amount || ',' || rec.type || ',' || rec.account_id || ',' || rec.creation_ts;
          UTL_FILE.PUT_LINE(v_file, v_line);
        END LOOP;

        UTL_FILE.FCLOSE(v_file);
      EXCEPTION
        WHEN OTHERS THEN
          IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
          END IF;
          RAISE;
      END;`
    );

    await connection.execute(
      `CREATE OR REPLACE PROCEDURE read_file(p_filename IN VARCHAR2, p_file_content OUT CLOB) IS
        l_file UTL_FILE.FILE_TYPE;
        l_line VARCHAR2(32767);
      BEGIN
        p_file_content := '';
        l_file := UTL_FILE.FOPEN('EXPORT_DIR', p_filename, 'R');

        LOOP
          BEGIN
            UTL_FILE.GET_LINE(l_file, l_line);
            p_file_content := p_file_content || l_line || CHR(10); -- CHR(10) est le caractère de nouvelle ligne
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              EXIT;
          END;
        END LOOP;

        UTL_FILE.FCLOSE(l_file);
      EXCEPTION
        WHEN UTL_FILE.INVALID_PATH THEN
          RAISE_APPLICATION_ERROR(-20001, 'Invalid file path');
        WHEN UTL_FILE.READ_ERROR THEN
          RAISE_APPLICATION_ERROR(-20004, 'File read error');
        WHEN OTHERS THEN
          RAISE_APPLICATION_ERROR(-20005, 'An error occurred: ' || SQLERRM);
      END;`
    );

    await connection.execute(
      `CREATE OR REPLACE PROCEDURE get_transactions_within_budget(
        p_account_id IN NUMBER,
        p_budget IN NUMBER,
        p_transactions OUT SYS_REFCURSOR
      ) AS
        v_total NUMBER := 0;
      BEGIN
        OPEN p_transactions FOR
          SELECT id, name, amount, type, account_id, creation_ts
          FROM (
            SELECT *,
                   SUM(amount) OVER (ORDER BY creation_ts) AS running_total
            FROM transactions
            WHERE account_id = p_account_id
            ORDER BY creation_ts
          )
          WHERE running_total <= p_budget;
      END;`
    );

    // TRIGGERS
    await connection.execute(
      `CREATE OR REPLACE TRIGGER trg_update_account_amount
      AFTER INSERT OR UPDATE OR DELETE ON transactions
      FOR EACH ROW
      BEGIN
        IF INSERTING THEN
          IF :NEW.type = 1 THEN -- In
            UPDATE accounts SET amount = amount + :NEW.amount WHERE id = :NEW.account_id;
          ELSIF :NEW.type = 0 THEN -- Out
            UPDATE accounts SET amount = amount - :NEW.amount WHERE id = :NEW.account_id;
          END IF;
        ELSIF UPDATING THEN
          IF :NEW.type = 1 THEN -- In
            UPDATE accounts SET amount = amount + :NEW.amount - :OLD.amount WHERE id = :NEW.account_id;
          ELSIF :NEW.type = 0 THEN -- Out
            UPDATE accounts SET amount = amount - :NEW.amount + :OLD.amount WHERE id = :NEW.account_id;
          END IF;
        ELSIF DELETING THEN
          IF :OLD.type = 1 THEN -- In
            UPDATE accounts SET amount = amount - :OLD.amount WHERE id = :OLD.account_id;
          ELSIF :OLD.type = 0 THEN -- Out
            UPDATE accounts SET amount = amount + :OLD.amount WHERE id = :OLD.account_id;
          END IF;
        END IF;
      END;`
    );

    // DATAS
    const usersSql = `INSERT INTO users (name, email, accounts) VALUES (:1, :2, 0)`;
    const usersRows = [
      ["Valentin Montagne", "contact@vm-it-consulting.com"],
      ["Amélie Dal", "amelie.dal@gmail.com"],
    ];
    let usersResult = await connection.executeMany(usersSql, usersRows);
    console.log(usersResult.rowsAffected, "Users rows inserted");

    const accountsSql = `INSERT INTO accounts (name, amount, user_id, transactions) VALUES (:1, :2, :3, 0)`;
    const accountsRows = [["Compte courant", 2000, 1]];
    let accountsResult = await connection.executeMany(
      accountsSql,
      accountsRows
    );
    console.log(accountsResult.rowsAffected, "Accounts rows inserted");

    await connection.commit();
  } catch (err) {
    console.error(err);
  }
}

module.exports = setupDatabase;
