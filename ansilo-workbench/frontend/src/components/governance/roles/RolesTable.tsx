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
import TablePagination from "@mui/material/TablePagination";
import TableRow from "@mui/material/TableRow";
import GovernanceMenu from "../GovernanceMenu";
import Link from "next/link";
import { Authenticated } from "../../auth/Authenticated";
import { useAppDispatch, useAppSelector } from "../../../store/hooks";
import { selectAuth } from "../../auth/auth.slice";
import { useEffect, useState } from "react";
import { executeQuery } from "../../sql/sql.api";

interface Role {
  username: string,
  description?: string
}

export const RolesTable = () => {
  const dispatch = useAppDispatch();
  const auth = useAppSelector(selectAuth);
  const [roles, setRoles] = useState<Role[]>()

  useEffect(() => {
    if (!auth.creds) {
      return;
    }

    (async () => {
      let res = await executeQuery(
        dispatch, auth.creds!, {
        sql: `
        SELECT
          usename as username,
          pg_catalog.shobj_description(usesysid, 'pg_authid') as desc
        FROM pg_catalog.pg_user
        WHERE usename NOT IN ('ansilosuperuser', 'ansiloadmin')
        ` }
      );

      setRoles(res.values.map(v => ({ username: v[0], description: v[1] === "NULL" ? "N/A" : v[1] }) as Role))
    })();
  }, [auth.creds])

  return (
    <Authenticated>
      <TableContainer>
        <Table stickyHeader sx={{ minWidth: 300 }}>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Description</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {roles?.length ? roles.map(role =>
              <TableRow>
                <TableCell sx={{ "& a": { color: "white" } }}>
                  <Link href={`/governance/roles/specific?username=${role.username}`}>{role.username}</Link>
                </TableCell>
                <TableCell>
                  {role.description}
                </TableCell>
              </TableRow>
            ) : <TableRow><TableCell colSpan={2}>No roles found</TableCell></TableRow>}
          </TableBody>
        </Table>
      </TableContainer>
    </Authenticated>
  );
};
