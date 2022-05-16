import { readFileSync } from "fs";
import type { NextApiHandler } from "next";

const dbHandler: NextApiHandler = async (request, response) => {
  // simulate IO latency
  await new Promise((resolve) => setTimeout(resolve, 500));

  const dbPath = process.env.NODE_INSTANCE + "/db.sqlite";
  const db = readFileSync(dbPath);

  response.setHeader("content-type", "application/x-sqlite3");
  response.setHeader("access-control-allow-origin", "*");
  response.write(db);
  response.status(200)
  response.end()
};

export default dbHandler;
