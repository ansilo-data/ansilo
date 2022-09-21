import _ from "lodash";
import { API_CONFIG } from "../../config/api";
import { AppDispatch } from "../../store/store";
import { AuthCredentials } from "../auth/auth.slice";
import { executeQuery } from "../sql/sql.api";
import { Job } from "./jobs.slice";

export const fetchJobs = async (
  dispatch: AppDispatch,
  auth: AuthCredentials
): Promise<Job[]> => {
  const res = await executeQuery(dispatch, auth, {
    sql: `
    SELECT
      j.id,
      j.name,
      j.description,
      j.service_user_id,
      j.sql,
      json_agg(t.cron) as triggers
    FROM ansilo_catalog.jobs j
    INNER JOIN ansilo_catalog.job_triggers t ON j.id = t.job_id
    GROUP BY j.id, j.name, j.description, j.service_user_id, j.sql
    `,
  });

  const jobs = res.values.map(
    (v) =>
      ({
        id: v[0] || "",
        name: v[1] === "NULL" ? v[0] : v[1],
        description: v[2] === "NULL" ? "N/A" : v[2],
        serviceUserId: v[3] === "NULL" ? "N/A" : v[3],
        query: { sql: v[4] },
        trigger: v[5],
        runs: [],
      } as Job)
  );

  return jobs;
};
