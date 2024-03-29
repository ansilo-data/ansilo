import Drawer from "@mui/material/Drawer";
import Toolbar from "@mui/material/Toolbar";
import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import Paper from "@mui/material/Paper";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import { useAppDispatch, useAppSelector } from "../../../../store/hooks";
import { selectAuth } from "../../../auth/auth.slice";
import { useEffect, useState } from "react";
import { executeQuery } from "../../../sql/sql.api";
import qs from 'qs';

export interface Grant {
  schema: string,
  table: string,
  grants: string
}

export const DataGrantsTable = () => {
  const dispatch = useAppDispatch();
  const auth = useAppSelector(selectAuth);
  const [grants, setGrants] = useState<Grant[]>()

  useEffect(() => {
    if (!auth.creds) {
      return;
    }

    let username = (qs.parse(window.location.search.substring(1)) || {})['username'];

    (async () => {
      let res = await executeQuery(
        dispatch, auth.creds!, {
        sql: `
        SELECT table_schema, table_name, string_agg(privilege_type, ', ') as grants
        FROM information_schema.role_table_grants
        WHERE grantee = $1
        AND table_schema != 'ansilo_catalog'
        GROUP BY grantee, table_schema, table_name;
        `,
        params: [String(username)]
      }
      );

      setGrants(res.values.map(v => ({ schema: v[0], table: v[1], grants: v[2] }) as Grant))
    })();
  }, [auth.creds, dispatch])

  return (
    <TableContainer sx={{ minWidth: 500 }}>
      <Table stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Schema</TableCell>
            <TableCell>Table</TableCell>
            <TableCell width="50%">Operations</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {grants?.length ?
            grants.map(grant => <TableRow key={grant.schema + '.' + grant.table}>
              <TableCell>{grant.schema}</TableCell>
              <TableCell>{grant.table}</TableCell>
              <TableCell>{grant.grants}</TableCell>
            </TableRow>)
            : <TableRow><TableCell colSpan={3}>No grants found</TableCell></TableRow>}
        </TableBody>
      </Table>
    </TableContainer>
  );
};
