import { readFileSync } from "fs";
import type { NextApiHandler } from "next";

const nodeHandler: NextApiHandler = async (request, response) => {
  // simulate IO latency
  await new Promise((resolve) => setTimeout(resolve, 500));

  const infoPath = process.env.NODE_INSTANCE + "/info.json";
  const json = JSON.parse(readFileSync(infoPath).toString("utf-8"));
  json.url = 'http://' + request.headers["host"];

  response.setHeader("content-type", "application/json");
  response.setHeader("access-control-allow-origin", "*");
  response.write(JSON.stringify(json));
  response.status(200);
  response.end();
};

export default nodeHandler;
