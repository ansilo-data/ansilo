import { CatalogState, Node } from "../catalog/catalog.slice";
import { Query, QueryResults } from "./sql.slice";
import initSqlJs, { SqlJsStatic } from "sql.js";

export const executeQuery = async (
  catalog: CatalogState,
  query: Query
): Promise<QueryResults> => {
  const SQL = await initSqlJs({
    locateFile: (file) =>
      `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.6.2/${file}`,
  });
  const db = new SQL.Database();
  const nodes = catalog.nodes || [];

  const files = await Promise.all(
    nodes.map((n) => createEmscriptenFile(SQL, n))
  );

  for (const [file, node] of files) {
    db.run(`ATTACH DATABASE '/${file}' AS ${node.name.replace(/ /g, '_').toLowerCase()};`);
  }

  const res = db.exec(query.sql);
  return res[0];
};

const createEmscriptenFile = async (
  SQL: SqlJsStatic,
  node: Node
): Promise<[string, Node]> => {
  const buff = await fetch(node.url + "/api/db")
    .then((r) => r.blob())
    .then((b) => b.arrayBuffer());

  const db = new SQL.Database(new Uint8Array(buff as ArrayBuffer));

  return [(db as any).filename, node];
};
