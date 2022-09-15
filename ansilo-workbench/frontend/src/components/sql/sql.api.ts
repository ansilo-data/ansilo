import { AppDispatch } from "../../store/store";
import { authenticatedFetch } from "../auth/auth.api";
import { AuthCredentials } from "../auth/auth.slice";
import { CatalogState, Node } from "../catalog/catalog.slice";
import { Query, QueryResults } from "./sql.slice";

export const executeQuery = async (
  dispatch: AppDispatch,
  creds: AuthCredentials,
  query: { sql: string; params?: string[] }
): Promise<QueryResults> => {
  let res = await authenticatedFetch(dispatch, creds, "/api/v1/query", {
    method: "post",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      sql: query.sql,
      params: query.params,
    }),
  });

  if (!res) {
    throw new Error(`Authentication failure`);
  }

  let results = await res.json();

  if (results.status === "error") {
    throw new Error(results.message || "Unknown error occurred");
  }

  return {
    columns: results.columns.map((c: any) => c[0]),
    values: results.data,
  };
};
