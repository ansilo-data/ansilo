import { readFileSync } from "fs";
import type { NextApiHandler } from "next";

const nodeHandler: NextApiHandler = async (request, response) => {
  // simulate IO latency
  await new Promise((resolve) => setTimeout(resolve, 500));

  const infoPath = process.env.NODE_INSTANCE + "/info.json";
  const json = readFileSync(infoPath);

  response.setHeader("content-type", "application/json");
  response.setHeader("access-control-allow-origin", "*");
  response.write(json);
  response.status(200)
  response.end()
};

export default nodeHandler;
