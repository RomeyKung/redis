const express = require("express");
const bodyparser = require("body-parser");
const mysql = require("mysql2/promise");
const redis = require("redis");
const cron = require("node-cron");

const app = express();

app.use(bodyparser.json());

const port = 8000;

let conn = null;
let redisConn = null;

// function init connection mysql
const initMySQL = async () => {
  conn = await mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "root",
    database: "tutorial",
  });
};

// function init connection redis
const initRedis = async () => {
  redisConn = redis.createClient();
  redisConn.on("error", (err) => console.log("Redis Client Error", err));
  await redisConn.connect();
};
//Lazy loading
app.get("/api/users", async (req, res) => {
  const cachedData = await redisConn.get("users");

  if (cachedData) {
    //ใช้ข้อมูล cache = cache hit
    const results = JSON.parse(cachedData);
    res.json({
      message: "Data from cache",
      cacheData: results,
    });
    return;
  }
  //cache miss
  const [results] = await conn.query("SELECT * FROM users");
  const userStringData = JSON.stringify(results);
  await redisConn.set("users", userStringData);
  res.json(results);
});

//Write through
app.get("/api/users2", async (req, res) => {
  const cachedData = await redisConn.get("users-2");
  if (cachedData) {
    //ใช้ข้อมูล cache = cache hit
    const results = JSON.parse(cachedData);
    res.json({
      message: "Data from cache2",
      cacheData: results,
    });
    return;
  }
  //cache miss
  const [results] = await conn.query("SELECT * FROM users");
  res.json(results);
});

app.post("/api/users2", async (req, res) => {
  let user = req.body;
  const [results] = await conn.query("INSERT INTO users SET ?", user);
  const cachedData = await redisConn.get("users-2");

  user.id = results.insertId;
  if (cachedData) {
    //update จาก cache
    const usersData = JSON.parse(cachedData);
    usersData.push(user);
    await redisConn.set("users-2", JSON.stringify(usersData));
  } else {
    //ดึงข้อมูลจาก db มาทำ cache ใหม่
    const [results] = await conn.query("SELECT * FROM users");
    await redisConn.set("users-2", JSON.stringify(results));
  }

  res.json({
    message: "Data inserted",
    results,
  });
});

app.get("/api/users3", async (req, res) => {
  const cachedData = await redisConn.get("users-2");
  if (cachedData) {
    //ใช้ข้อมูล cache = cache hit
    const results = JSON.parse(cachedData);
    res.json({
      message: "Data from cache3",
      cacheData: results,
    });
    return;
  }
  //cache miss
  const [results] = await conn.query("SELECT * FROM users");
  res.json(results);
});

//Write back

app.put("/api/users3/:id", async (req, res) => {
  let user = req.body;
  let id = parseInt(req.params.id);
  user.id = id;
  const cachedData = await redisConn.get("users-3");
  let userUpdateIndex = (await redisConn.get("user-update-index")) || [];

  if (cachedData) {
    //update จาก cache
    const results = JSON.parse(cachedData);
    const selectedIndex = results.findIndex((user) => user.id === id);
    results[selectedIndex] = user;
    userUpdateIndex.push(selectedIndex);
    await redisConn.set("users-3", JSON.stringify(results));
  } else {
    //ดึงข้อมูลจาก db มาทำ cache ใหม่
    const [results] = await conn.query("SELECT * FROM users");
    const selectedIndex = results.findIndex((user) => user.id === id);
    results[selectedIndex] = user;
    userUpdateIndex.push(selectedIndex);
    await redisConn.set("users-3", JSON.stringify(results));
  }

  await redisConn.set("user-update-index", JSON.stringify(userUpdateIndex));

  res.json({
    message: "Data updated",
    user,
  });
});

//run cron job every 5 seconds
cron.schedule("*/5 * * * * *", async () => {
  const cachedDataString = await redisConn.get("users-3");
  const userUpdateIndexString = await redisConn.get("user-update-index");

  const cachedData = JSON.parse(cachedDataString);
  const userUpdateIndex = JSON.parse(userUpdateIndexString);

  //ดึง userUpdateIndex ที่มาการ update จาก redis มา update ลงใน database
  if (userUpdateIndex.length > 0) {
    // เราจะ update ใน database

    for (let i = 0; i < userUpdateIndex.length; i++) {
      const id = cachedData[userUpdateIndex[i]].id;
      const updateUser = {
        name: cachedData[userUpdateIndex[i]].name,
        email: cachedData[userUpdateIndex[i]].age,
        description: cachedData[userUpdateIndex[i]].description,
      };

      const [results] = await conn.query("UPDATE users SET ? WHERE id = ?", [
        updateUser,
        id,
      ]);
      
      console.log("update user: ", updateUser);
    }

    //clear user-update-index เพื่อจะได้ไม่ update ซ้ำ
    await redisConn.del("user-update-index");
  }
});

//set time live for cache
app.get("/api/users4", async (req, res) => {
  const cachedData = await redisConn.get("users-4");
  if (cachedData) {
    //ใช้ข้อมูล cache = cache hit
    const results = JSON.parse(cachedData);
    res.json({
      message: "Data from cache4",
      cacheData: results,
    });
    return;
  }
  //cache miss
  const [results] = await conn.query("SELECT * FROM users");
  await redisConn.set("users-4", JSON.stringify(results), "EX", 10);
  res.json(results);
});

app.listen(port, async (req, res) => {
  await initMySQL();
  await initRedis();
  console.log("http server run at " + port);
});
