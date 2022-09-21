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

interface ServiceUser {
  id: string,
  username: string,
  description?: string
}

export const ServiceUsersTable = () => {
  const dispatch = useAppDispatch();
  const auth = useAppSelector(selectAuth);
  const [users, setUsers] = useState<ServiceUser[]>()

  useEffect(() => {
    if (!auth.creds) {
      return;
    }

    (async () => {
      let res = await executeQuery(
        dispatch, auth.creds!, {
        sql: `
        SELECT
          id,
          username,
          description
        FROM ansilo_catalog.service_users
        ` }
      );

      setUsers(res.values.map(v => ({ id: v[0], username: v[1], description: v[2] === "NULL" ? "N/A" : v[2] }) as ServiceUser))
    })();
  }, [auth.creds, dispatch])

  return (
    <Authenticated>
      <TableContainer>
        <Table stickyHeader sx={{ minWidth: 300 }}>
          <TableHead>
            <TableRow>
              <TableCell>Id</TableCell>
              <TableCell>Username</TableCell>
              <TableCell>Description</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {users?.length ? users.map(user =>
              <TableRow key={user.id}>
                <TableCell>
                  {user.id}
                </TableCell>
                <TableCell>
                  {user.username}
                </TableCell>
                <TableCell>
                  {user.description}
                </TableCell>
              </TableRow>
            ) : <TableRow><TableCell colSpan={3}>No service users found</TableCell></TableRow>}
          </TableBody>
        </Table>
      </TableContainer>
    </Authenticated>
  );
};
