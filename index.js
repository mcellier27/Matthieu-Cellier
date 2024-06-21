const oracledb = require("oracledb");
const express = require("express");
const path = require("path");
const setupDatabase = require("./setupDatabase");
const app = express();

// Set EJS as the view engine
app.set("view engine", "ejs");

// Define the directory where your HTML files (views) are located
app.set("views", path.join(__dirname, "views"));

// Optionally, you can define a static files directory (CSS, JS, images, etc.)
app.use(express.static(path.join(__dirname, "public")));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;

let connection;

async function connectToDatabase() {
  console.log("====================");
  try {
    connection = await oracledb.getConnection({
      user: "admin",
      password: "password",
      connectionString: "localhost:1525/XEPDB1", // Assurez-vous que cette connexion est correcte
    });
    console.log("Successfully connected to Oracle Database");
  } catch (err) {
    console.error(err);
  }
}

// Define a route to render the HTML file
app.get("/", async (req, res) => {
  res.render("index"); // Assuming you have an "index.ejs" file in the "views" directory
});

app.get("/users", async (req, res) => {
  try {
    const getUsersSQL = `SELECT * FROM users`;
    const result = await connection.execute(getUsersSQL);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send("Error retrieving users");
  }
});

app.post("/users", async (req, res) => {
  try {
    const createUserSQL = `BEGIN
      insert_user(:name, :email, :user_id);
    END;`;
    const result = await connection.execute(createUserSQL, {
      name: req.body.name,
      email: req.body.email,
      user_id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER },
    });

    if (result.outBinds && result.outBinds.user_id) {
      res.redirect(`/views/${result.outBinds.user_id}`);
    } else {
      res.sendStatus(500);
    }
  } catch (err) {
    console.error(err);
    res.status(500).send("Error creating user");
  }
});

app.get("/views/:userId", async (req, res) => {
  try {
    const getCurrentUserSQL = `SELECT * FROM users WHERE id = :1`;
    const getAccountsSQL = `SELECT * FROM accounts WHERE user_id = :1`;
    const [currentUser, accounts] = await Promise.all([
      connection.execute(getCurrentUserSQL, [req.params.userId]),
      connection.execute(getAccountsSQL, [req.params.userId]),
    ]);

    res.render("user-view", {
      currentUser: currentUser.rows[0],
      accounts: accounts.rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).send("Error retrieving user data");
  }
});

app.get("/views/:userId/:accountId", async (req, res) => {
  try {
    const getCurrentUserSQL = `SELECT * FROM users WHERE id = :1`;
    const getCurrentAccountSQL = `SELECT * FROM accounts WHERE id = :1 AND user_id = :2`;
    const getTransactionsSQL = `SELECT * FROM transactions WHERE account_id = :1`;
    const [currentUser, currentAccount, transactions] = await Promise.all([
      connection.execute(getCurrentUserSQL, [req.params.userId]),
      connection.execute(getCurrentAccountSQL, [req.params.accountId, req.params.userId]),
      connection.execute(getTransactionsSQL, [req.params.accountId]),
    ]);

    res.render("transaction-view", {
      currentUser: currentUser.rows[0],
      currentAccount: currentAccount.rows[0],
      transactions: transactions.rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).send("Error retrieving account data");
  }
});

app.get("/accounts", async (req, res) => {
  try {
    const getAccountsSQL = `SELECT * FROM accounts`;
    const result = await connection.execute(getAccountsSQL);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send("Error retrieving accounts");
  }
});

app.post("/accounts", async (req, res) => {
  try {
    const createAccountSQL = `BEGIN
      insert_account(:name, :amount, :user_id);
    END;`;
    await connection.execute(createAccountSQL, {
      name: req.body.name,
      amount: req.body.amount,
      user_id: req.body.user_id,
    });

    await connection.commit();
    res.redirect(`/views/${req.body.user_id}`);
  } catch (err) {
    console.error(err);
    res.status(500).send("Error creating account");
  }
});

app.post("/transactions", async (req, res) => {
  try {
    const createTransactionSQL = `BEGIN
      insert_transaction(:name, :amount, :type, :account_id);
    END;`;
    await connection.execute(createTransactionSQL, {
      name: req.body.name,
      amount: req.body.amount,
      type: req.body.type,
      account_id: req.body.account_id,
    });

    await connection.commit();
    res.redirect(`/views/${req.body.user_id}`);
  } catch (err) {
    console.error(err);
    res.status(500).send("Error creating transaction");
  }
});

// Route pour exporter les transactions d'un compte dans un fichier CSV
app.post("/accounts/:accountId/exports", async (req, res) => {
  const accountId = parseInt(req.params.accountId, 10);
  try {
    const exportSQL = `BEGIN export_transactions_to_csv(:accountId); END;`;
    await connection.execute(exportSQL, { accountId });
    await connection.commit();
    res.status(200).send("CSV export generated successfully.");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error generating CSV export");
  }
});

// Route pour lire le fichier CSV et renvoyer son contenu
app.get("/accounts/:accountId/exports", async (req, res) => {
  try {
    const exportsSQL = `BEGIN read_file('transactions.csv', :content); END;`;
    const result = await connection.execute(exportsSQL, {
      content: { dir: oracledb.BIND_OUT, type: oracledb.CLOB },
    });
    const data = await result.outBinds.content.getData();
    res.header("Content-Type", "text/csv");
    res.attachment('transactions.csv');
    res.send(data);
  } catch (err) {
    console.error(err);
    res.status(500).send("Error reading CSV file");
  }
});

// Route to get transactions within budget
app.get("/accounts/:accountId/budgets/:amount", async (req, res) => {
  const accountId = parseInt(req.params.accountId, 10);
  const budget = parseFloat(req.params.amount);

  try {
    const result = await connection.execute(
      `BEGIN
        get_transactions_within_budget(:accountId, :budget, :cursor);
      END;`,
      {
        accountId: accountId,
        budget: budget,
        cursor: { type: oracledb.CURSOR, dir: oracledb.BIND_OUT },
      }
    );

    const cursor = result.outBinds.cursor;
    const transactions = [];
    let row;

    while ((row = await cursor.getRow())) {
      transactions.push(row);
    }

    await cursor.close();
    res.json(transactions);
  } catch (err) {
    console.error(err);
    res.status(500).send("Error retrieving transactions within budget");
  }
});

connectToDatabase().then(async () => {
  await setupDatabase(connection);
  console.log("====================");
  // Start the server
  app.listen(3000, () => {
    console.log("Server started on http://localhost:3000");
  });
});

module.exports = app;
